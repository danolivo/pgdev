#include "mchar.h"
#include "mb/pg_wchar.h"

#include "catalog/pg_collation.h"
#include "utils/selfuncs.h"
#include "utils/memutils.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/supportnodes.h"
#include "regex/regex.h"

/*
**  Originally written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
**  Rich $alz is now <rsalz@bbn.com>.
**  Special thanks to Lars Mathiesen <thorinn@diku.dk> for the LABORT code.
**
**  This code was shamelessly stolen from the "pql" code by myself and
**  slightly modified :)
**
**  All references to the word "star" were replaced by "percent"
**  All references to the word "wild" were replaced by "like"
**
**  All the nice shell RE matching stuff was replaced by just "_" and "%"
**
**  As I don't have a copy of the SQL standard handy I wasn't sure whether
**  to leave in the '\' escape character handling.
**
**  Keith Parks. <keith@mtcc.demon.co.uk>
**
**  SQL92 lets you specify the escape character by saying
**  LIKE <pattern> ESCAPE <escape character>. We are a small operation
**  so we force you to use '\'. - ay 7/95
**
**  Now we have the like_escape() function that converts patterns with
**  any specified escape character (or none at all) to the internal
**  default escape character, which is still '\'. - tgl 9/2000
**
** The code is rewritten to avoid requiring null-terminated strings,
** which in turn allows us to leave out some memcpy() operations.
** This code should be faster and take less memory, but no promises...
** - thomas 2000-08-06
**
** Adopted for UTF-16 by teodor
*/

#define LIKE_TRUE                       1
#define LIKE_FALSE                      0
#define LIKE_ABORT                      (-1)

/*
 * Size of the on-stack buffer used to lowercase short LIKE operands.  Chosen
 * as a small multiple of the common document-number / identifier size; larger
 * inputs fall back to palloc.
 */
#define MCHAR_LOWER_STACK_UCHARS		512

/*
 * Case-insensitive equality on a single UTF-16 character (one or two UChars).
 * Used in the original per-row ICU-collation path; retained only for the
 * legacy MatchUChar() that walks raw (non-lowered) input.
 */
static int
uchareq(UChar *p1, UChar *p2) {
	int l1=0, l2=0;
	/*
	 * Count length of char:
	 * We suppose that string is correct!!
	 */
	U16_FWD_1(p1, l1, 2);
	U16_FWD_1(p2, l2, 2);

	return (UCharCaseCompare(p1, l1, p2, l2)==0) ? 1 : 0;
}

/*
 * Byte-exact equality on a single UTF-16 character, assuming both operands
 * have already been passed through u_strToLower().  No ICU collator involved.
 */
static inline int
uchareq_lowered(const UChar *p1, const UChar *p2)
{
	int l1 = 0, l2 = 0;

	U16_FWD_1_UNSAFE(p1, l1);
	U16_FWD_1_UNSAFE(p2, l2);

	if (l1 != l2)
		return 0;
	return (u_memcmp(p1, p2, l1) == 0) ? 1 : 0;
}

#define NextChar(p, plen) 			\
	do {							\
		int __l = 0;				\
		U16_FWD_1((p), __l, (plen));\
		(p) +=__l;					\
		(plen) -=__l;				\
	} while(0)

#define CopyAdvChar(dst, src, srclen) 	\
	do { 								\
		int __l = 0;					\
		U16_FWD_1((src), __l, (srclen));\
		(srclen) -= __l;				\
		while (__l-- > 0)				\
			*(dst)++ = *(src)++;		\
	} while (0)


static UChar	UCharPercent = 0;
static UChar	UCharBackSlesh = 0;
static UChar	UCharUnderLine = 0;
static UChar	UCharStar = 0;
static UChar	UCharDotDot = 0;
static UChar	UCharUp = 0;
static UChar	UCharLBracket = 0;
static UChar	UCharQ = 0;
static UChar	UCharRBracket = 0;
static UChar	UCharDollar = 0;
static UChar	UCharDot = 0;
static UChar	UCharLFBracket = 0;
static UChar	UCharRFBracket = 0;
static UChar	UCharQuote = 0;
static UChar	UCharSpace = 0;
static UChar	UCharOne = 0;
static UChar	UCharComma = 0;
static UChar	UCharLQBracket = 0;
static UChar	UCharRQBracket = 0;

#define MkUChar(uc, c) 	do {			\
	char __c = (c); 					\
	u_charsToUChars( &__c, &(uc), 1 );	\
} while(0)

#define	SET_UCHAR	if ( UCharPercent == 0 ) {	\
		MkUChar( UCharPercent,		'%' );		\
		MkUChar( UCharBackSlesh,	'\\' );		\
		MkUChar( UCharUnderLine,	'_' );		\
		MkUChar( UCharStar,			'*' );		\
		MkUChar( UCharDotDot,		':' );		\
		MkUChar( UCharUp,			'^' );		\
		MkUChar( UCharLBracket,		'(' );		\
		MkUChar( UCharQ,			'?' );		\
		MkUChar( UCharRBracket,		')' );		\
		MkUChar( UCharDollar,		'$' );		\
		MkUChar( UCharDot,			'.' );		\
		MkUChar( UCharLFBracket,	'{' );		\
		MkUChar( UCharRFBracket,	'}' );		\
		MkUChar( UCharQuote,		'"' );		\
		MkUChar( UCharSpace,		' ' );		\
		MkUChar( UCharOne,			'1' );		\
		MkUChar( UCharComma,		',' );		\
		MkUChar( UCharLQBracket,	'[' );		\
		MkUChar( UCharRQBracket,	']' );		\
	}

int
m_isspace(UChar c) {
	SET_UCHAR;

	return (c == UCharSpace);
}

/*
 * Removes trailing spaces in '111 %' pattern.  Returns the input pointer
 * when no trimming is needed, otherwise a freshly palloc'd buffer owned by
 * the caller.  Never mutates src.
 */
static UChar *
removeTrailingSpaces(const UChar *src, int srclen, int *dstlen,
					 bool *isSpecialLast)
{
	UChar	   *dst = (UChar *) src;
	const UChar *ptr;
	UChar	   *dptr;
	const UChar *markptr;

	*dstlen = srclen;
	ptr = src + srclen - 1;
	SET_UCHAR;

	*isSpecialLast = (srclen > 0 &&
					  (u_isspace(*ptr) ||
					   *ptr == UCharPercent ||
					   *ptr == UCharUnderLine));
	while (ptr >= src)
	{
		if (*ptr == UCharPercent || *ptr == UCharUnderLine)
		{
			if (ptr == src)
				return dst;		/* first character */

			if (*(ptr - 1) == UCharBackSlesh)
				return dst;		/* use src as is */

			if (u_isspace(*(ptr - 1)))
			{
				ptr--;
				break;			/* % or _ is after space which should be removed */
			}
		}
		else
		{
			return dst;
		}
		ptr--;
	}

	markptr = ptr + 1;
	dst = (UChar *) palloc(sizeof(UChar) * srclen);

	/* find last non-space character */
	while (ptr >= src && u_isspace(*ptr))
		ptr--;

	dptr = dst + (ptr - src + 1);

	if (ptr >= src)
		memcpy(dst, src, sizeof(UChar) * (ptr - src + 1));

	while (markptr - src < srclen)
	{
		*dptr = *markptr;
		dptr++;
		markptr++;
	}

	*dstlen = dptr - dst;
	return dst;
}

/*
 * Right-pad an mchar value with spaces up to its declared typmod length.
 * Returns src->data unchanged when no padding is needed; otherwise a
 * freshly palloc'd buffer owned by the caller.
 */
static UChar *
addTrailingSpace(const MChar *src, int *newlen)
{
	int			scharlen = u_countChar32(src->data, UCHARLENGTH(src));

	if (src->typmod > scharlen)
	{
		UChar	   *res = (UChar *) palloc(sizeof(UChar) *
										   (UCHARLENGTH(src) + src->typmod));

		memcpy(res, src->data, sizeof(UChar) * UCHARLENGTH(src));
		FillWhiteSpace(res + UCHARLENGTH(src), src->typmod - scharlen);

		*newlen = src->typmod;

		return res;
	}
	else
	{
		*newlen = UCHARLENGTH(src);
		return (UChar *) src->data;
	}
}

/*
 * Per-call-site cache of the lowered LIKE pattern.  The LIKE pattern is
 * usually a Const that never changes for the life of a scan, so lowering it
 * once and stashing the result in fn_extra is a large win compared with
 * re-lowering every row.
 *
 * We validate the cache with (src, src_len): if the caller hands us the
 * same MVarChar/MChar datum pointer and byte length, the content is the same
 * and the cached lowered bytes are reusable.  A non-constant pattern simply
 * misses the cache every row but still benefits from the lowered-once path
 * within a single call.
 */
typedef struct MCharPatternCache
{
	const UChar	   *src;
	int32			src_len;
	UChar		   *lowered;	/* palloc'd in fn_mcxt */
	int32			lowered_len;
	bool			is_simple_substring;	/* pattern is %literal% */
	const UChar	   *literal;	/* inner literal for fast path */
	int32			literal_len;
	/*
	 * True when every code point in the lowered pattern is its own upper-
	 * and lower-case (digits, punctuation, non-cased scripts).  If so, no
	 * text code point can match unless it is byte-identical to a pattern
	 * code point, and we can skip lowering the text at call time.
	 */
	bool			case_neutral;
	/*
	 * mchar_like only: set when the pattern ends with space/%/_ and the
	 * mchar text must therefore be right-padded to its typmod length before
	 * matching (see removeTrailingSpaces()).  Unused for mvarchar_like.
	 */
	bool			needs_text_pad;
} MCharPatternCache;

/*
 * Lowercase a UTF-16 string into dst.  Returns the lowered length.  On
 * U_BUFFER_OVERFLOW_ERROR the caller is expected to grow the buffer and
 * retry.  Any other ICU failure raises ereport(ERROR).
 */
static int
lowercase_into(UChar *dst, int dstsz, const UChar *src, int srclen,
			   UErrorCode *err_out)
{
	UErrorCode	err = U_ZERO_ERROR;
	int			n;

	n = u_strToLower(dst, dstsz, src, srclen, "", &err);
	*err_out = err;
	if (U_FAILURE(err) && err != U_BUFFER_OVERFLOW_ERROR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("mchar: u_strToLower failed: %s", u_errorName(err))));
	return n;
}

/*
 * Examine a lowered LIKE pattern and decide:
 *   1. whether it has the "%literal%" shape with no wildcards or escapes
 *      in between (the single-u_strFindFirst fast path), and
 *   2. whether every code point is case-neutral (allowing the text side to
 *      bypass u_strToLower() at match time).
 *
 * Called once per cache build, not a hot path.
 */
static void
classify_pattern(MCharPatternCache *cache)
{
	const UChar	   *buf = cache->lowered;
	int				buflen = cache->lowered_len;
	int				i;
	int				pos;
	bool			neutral;

	SET_UCHAR;
	cache->is_simple_substring = false;
	cache->literal = NULL;
	cache->literal_len = 0;
	cache->case_neutral = false;

	/*
	 * Case-neutrality: walk the (code-point-decoded) pattern and check that
	 * every code point equals its own upper- and lower-case.  buf is the
	 * output of u_strToLower(), hence well-formed UTF-16, so U16_NEXT_UNSAFE
	 * is safe.
	 */
	pos = 0;
	neutral = true;
	while (pos < buflen)
	{
		UChar32		c;

		U16_NEXT_UNSAFE(buf, pos, c);
		if (u_tolower(c) != c || u_toupper(c) != c)
		{
			neutral = false;
			break;
		}
	}
	cache->case_neutral = neutral;

	if (buflen < 2 || buf[0] != UCharPercent || buf[buflen - 1] != UCharPercent)
		return;

	for (i = 1; i < buflen - 1; i++)
	{
		if (buf[i] == UCharPercent ||
			buf[i] == UCharUnderLine ||
			buf[i] == UCharBackSlesh)
			return;
	}

	cache->is_simple_substring = true;
	cache->literal = buf + 1;
	cache->literal_len = buflen - 2;
}

/*
 * Return the cached lowered pattern for (src, srclen), rebuilding the cache
 * entry if the pattern has changed.  Returns NULL when no FmgrInfo is
 * available (e.g. DirectFunctionCall) -- callers then lower into a local
 * buffer instead of the cache.
 *
 * When trim_trailing_spaces is true (mchar_like), removeTrailingSpaces() is
 * run on the raw pattern first and needs_text_pad is populated so the
 * caller can right-pad the text per row.  When false (mvarchar_like), the
 * raw pattern is lowered as-is and needs_text_pad remains false.
 */
static MCharPatternCache *
ensure_pattern_cache(FmgrInfo *flinfo, const UChar *src, int srclen,
					 bool trim_trailing_spaces)
{
	MCharPatternCache  *cache;
	const UChar		   *pattern_src;
	int					pattern_len;
	UChar			   *trimmed = NULL;
	bool				need_pad = false;
	UErrorCode			err;
	UChar			   *buf;
	int					bufsz;
	int					buflen;

	if (flinfo == NULL)
		return NULL;

	cache = (MCharPatternCache *) flinfo->fn_extra;
	if (cache != NULL &&
		cache->src == src && cache->src_len == srclen)
		return cache;

	if (cache == NULL)
	{
		cache = (MCharPatternCache *)
			MemoryContextAllocZero(flinfo->fn_mcxt, sizeof(*cache));
		flinfo->fn_extra = cache;
	}
	else if (cache->lowered != NULL)
	{
		pfree(cache->lowered);
		cache->lowered = NULL;
		cache->lowered_len = 0;
		cache->src = NULL;
		cache->src_len = 0;
	}

	/*
	 * For mchar_like, strip trailing spaces (and any trailing wildcard that
	 * follows them) up front so the trimmed form is what gets cached and
	 * matched row by row.  removeTrailingSpaces() allocates only when it
	 * actually trims; otherwise it returns the input pointer.  Switch into
	 * fn_mcxt so any fresh allocation survives until we pfree it below.
	 */
	if (trim_trailing_spaces)
	{
		MemoryContext	oldctx;

		oldctx = MemoryContextSwitchTo(flinfo->fn_mcxt);
		trimmed = removeTrailingSpaces(src, srclen,
									   &pattern_len, &need_pad);
		MemoryContextSwitchTo(oldctx);
		pattern_src = trimmed;
	}
	else
	{
		pattern_src = src;
		pattern_len = srclen;
	}

	/*
	 * Allocate a generous first guess (2x + pad).  Case folding can expand a
	 * code point (e.g. German sharp-S -> SS), so pattern_len alone is not
	 * always enough.  If ICU signals overflow we grow precisely and retry.
	 */
	bufsz = pattern_len * 2 + 4;
	buf = (UChar *) MemoryContextAlloc(flinfo->fn_mcxt, bufsz * sizeof(UChar));
	buflen = lowercase_into(buf, bufsz, pattern_src, pattern_len, &err);
	if (err == U_BUFFER_OVERFLOW_ERROR)
	{
		pfree(buf);
		bufsz = buflen + 4;
		buf = (UChar *) MemoryContextAlloc(flinfo->fn_mcxt,
										   bufsz * sizeof(UChar));
		buflen = lowercase_into(buf, bufsz, pattern_src, pattern_len, &err);
		Assert(!U_FAILURE(err));
	}

	/* trimmed is only live when removeTrailingSpaces() actually trimmed. */
	if (trimmed != NULL && trimmed != src)
		pfree(trimmed);

	cache->lowered = buf;
	cache->lowered_len = buflen;
	cache->src = src;
	cache->src_len = srclen;
	cache->needs_text_pad = need_pad;
	classify_pattern(cache);

	return cache;
}

/*
 * Lower (src, srclen) into a working buffer for the text side of LIKE.
 *
 * Uses the caller-provided stack buffer when the lowered form fits, falls
 * back to palloc in CurrentMemoryContext otherwise.  For ASCII-only input we
 * skip the ICU call entirely and fold [A-Z]|=0x20 inline -- that loop
 * auto-vectorises and is roughly an order of magnitude cheaper than
 * u_strToLower() on typical short identifier columns.
 *
 * Writes *allocated so the caller knows whether to pfree.
 */
static UChar *
lowercase_text(const UChar *src, int srclen,
			   UChar *stack_buf, int stack_sz,
			   int *lowered_len, bool *allocated)
{
	UErrorCode	err;
	UChar	   *buf;
	int			bufsz;
	int			n;
	int			i;
	bool		all_ascii = true;

	Assert(stack_sz > 0);
	*allocated = false;

	/*
	 * ASCII folding cannot expand, so srclen UChars is the exact output size
	 * on the fast path.  Pick a buffer up front; the loop below writes
	 * through it and only falls back to ICU if a non-ASCII code unit shows
	 * up, in which case ICU overwrites whatever partial result we left here.
	 */
	bufsz = srclen * 2 + 4;
	if (bufsz <= stack_sz)
	{
		buf = stack_buf;
		bufsz = stack_sz;
	}
	else
	{
		buf = (UChar *) palloc(bufsz * sizeof(UChar));
		*allocated = true;
	}

	for (i = 0; i < srclen; i++)
	{
		UChar		c = src[i];

		if (c >= 0x80)
		{
			all_ascii = false;
			break;
		}
		buf[i] = (c >= 'A' && c <= 'Z') ? (UChar) (c | 0x20) : c;
	}
	if (all_ascii)
	{
		*lowered_len = srclen;
		return buf;
	}

	n = lowercase_into(buf, bufsz, src, srclen, &err);
	if (err == U_BUFFER_OVERFLOW_ERROR)
	{
		if (*allocated)
			pfree(buf);
		bufsz = n + 4;
		buf = (UChar *) palloc(bufsz * sizeof(UChar));
		*allocated = true;
		n = lowercase_into(buf, bufsz, src, srclen, &err);
		Assert(!U_FAILURE(err));
	}
	*lowered_len = n;
	return buf;
}

/*
 * LIKE matcher that assumes both t and p have already been passed through
 * u_strToLower() at the root locale (or equivalently the pattern is known to
 * be case-neutral and t is therefore acceptable as-is).  Recursion re-enters
 * this function directly, so we do not re-lower on every wildcard retry.
 *
 * WARNING: this intentionally drops the collation-aware semantics of the
 * legacy MatchUChar() path.  It will NOT match code points that differ only
 * in Unicode normalisation (NFC vs NFD) or in locale-specific foldings such
 * as German sharp-s <-> "ss".  Callers relying on those equivalences must
 * normalise their input up front.
 */
static int
match_lowered(const UChar *t, int tlen, const UChar *p, int plen)
{
	SET_UCHAR;

	/* Fast path for match-everything pattern */
	if ((plen == 1) && (*p == UCharPercent))
		return LIKE_TRUE;

	while ((tlen > 0) && (plen > 0))
	{
		if (*p == UCharBackSlesh)
		{
			/* Next pattern char must match literally, whatever it is */
			NextChar(p, plen);
			if ((plen <= 0) || !uchareq_lowered(t, p))
				return LIKE_FALSE;
		}
		else if (*p == UCharPercent)
		{
			/* %% is the same as % per the SQL standard */
			while ((plen > 0) && (*p == UCharPercent))
				NextChar(p, plen);
			if (plen <= 0)
				return LIKE_TRUE;

			while (tlen > 0)
			{
				if (uchareq_lowered(t, p) ||
					(*p == UCharBackSlesh) ||
					(*p == UCharUnderLine))
				{
					int matched = match_lowered(t, tlen, p, plen);

					if (matched != LIKE_FALSE)
						return matched;
				}
				NextChar(t, tlen);
			}
			return LIKE_ABORT;
		}
		else if ((*p != UCharUnderLine) && !uchareq_lowered(t, p))
		{
			return LIKE_FALSE;
		}

		NextChar(t, tlen);
		NextChar(p, plen);
	}

	if (tlen > 0)
		return LIKE_FALSE;

	while ((plen > 0) && (*p == UCharPercent))
		NextChar(p, plen);
	if (plen <= 0)
		return LIKE_TRUE;

	return LIKE_ABORT;
}

/*
 * Execute LIKE using the cached lowered pattern.
 *
 *   - For the %literal% shape with literal_len > tlen, reject immediately
 *     without touching the text.
 *   - For a case-neutral pattern (digits, CJK, punctuation, ...), skip the
 *     per-row text lowering: no text code point can match unless it is
 *     byte-identical to a pattern code point, so raw text is effectively
 *     pre-lowered for this match.
 *   - Otherwise lower the text once into a stack buffer (palloc only for
 *     long inputs) and then run the simple-substring or wildcard matcher.
 */
static bool
execute_like_cached(const UChar *text, int tlen, MCharPatternCache *cache)
{
	UChar		tstack[MCHAR_LOWER_STACK_UCHARS];
	UChar	   *tbuf;
	int			tlen_lower;
	bool		tbuf_allocated = false;
	bool		result;

	Assert(cache != NULL);
	Assert(cache->lowered != NULL || cache->lowered_len == 0);
	Assert(!cache->is_simple_substring ||
		   (cache->literal != NULL || cache->literal_len == 0));

	/* Cheap early reject: %literal% with literal longer than text. */
	if (cache->is_simple_substring && cache->literal_len > tlen)
		return false;

	if (cache->case_neutral)
	{
		tbuf = (UChar *) text;
		tlen_lower = tlen;
	}
	else
	{
		tbuf = lowercase_text(text, tlen,
							  tstack, MCHAR_LOWER_STACK_UCHARS,
							  &tlen_lower, &tbuf_allocated);
	}

	if (cache->is_simple_substring)
	{
		if (cache->literal_len == 0)
			result = true;
		else if (cache->literal_len > tlen_lower)
			result = false;
		else
			result = (u_strFindFirst(tbuf, tlen_lower,
									 cache->literal,
									 cache->literal_len) != NULL);
	}
	else
	{
		int r = match_lowered(tbuf, tlen_lower,
							  cache->lowered, cache->lowered_len);
		result = (r == LIKE_TRUE);
	}

	if (tbuf_allocated)
		pfree(tbuf);
	return result;
}

static int
MatchUChar(UChar *t, int tlen, UChar *p, int plen) {
	SET_UCHAR;

	/* Fast path for find substring pattern */
	if ((plen >= 2) && p[0] == UCharPercent && p[plen-1] == UCharPercent && !u_strFindFirst(p+1, plen-2, &UCharPercent, 1) && !u_strFindFirst(p+1, plen-2, &UCharBackSlesh, 1) && !u_strFindFirst(p+1, plen-2, &UCharUnderLine, 1))
	{
		if (plen-2 > tlen)
			return LIKE_FALSE;

 		if (tlen > 100 || plen > 100)
		{
			UChar* tbuf;
			UChar* pbuf;
			int    tbufsz = tlen + 512;
			int    pbufsz = plen + 512;
			int    tbuflen;
			int    pbuflen;
			bool   found;
			UErrorCode status1 = U_ZERO_ERROR;
			UErrorCode status2 = U_ZERO_ERROR;

			tbuf = malloc(tbufsz*sizeof(UChar));
			pbuf = malloc(pbufsz*sizeof(UChar));
			tbuflen = u_strToLower(tbuf, tbufsz, t,   tlen,   NULL, &status1);
			pbuflen = u_strToLower(pbuf, pbufsz, p+1, plen-2, NULL, &status2);
			if (tbuflen < tbufsz && pbuflen < pbufsz && U_SUCCESS(status1) && U_SUCCESS(status2))
			{
				found = u_strFindFirst(tbuf, tbuflen, pbuf, pbuflen);
				free(tbuf);
				free(pbuf);
				return found ? LIKE_TRUE : LIKE_FALSE;
			}
			free(tbuf);
			free(pbuf);
		}
	}

	/* Fast path for match-everything pattern */
	if ((plen == 1) && (*p == UCharPercent))
		return LIKE_TRUE;

	while ((tlen > 0) && (plen > 0)) {
		if (*p == UCharBackSlesh) {
			/* Next pattern char must match literally, whatever it is */
			NextChar(p, plen);
			if ((plen <= 0) || !uchareq(t, p))
				return LIKE_FALSE;
		} else if (*p == UCharPercent) {
			/* %% is the same as % according to the SQL standard */
			/* Advance past all %'s */
			while ((plen > 0) && (*p == UCharPercent))
				NextChar(p, plen);
			/* Trailing percent matches everything. */
			if (plen <= 0)
				return LIKE_TRUE;

			/*
			 * Otherwise, scan for a text position at which we can match the
			 * rest of the pattern.
			 */
			while (tlen > 0) {
				/*
				 * Optimization to prevent most recursion: don't recurse
				 * unless first pattern char might match this text char.
				 */
				if (uchareq(t, p) || (*p == UCharBackSlesh) || (*p == UCharUnderLine)) {
					int         matched = MatchUChar(t, tlen, p, plen);

					if (matched != LIKE_FALSE)
						return matched; /* TRUE or ABORT */
				}

				NextChar(t, tlen);
			}

			/*
			 * End of text with no match, so no point in trying later places
			 * to start matching this pattern.
			 */
			return LIKE_ABORT;
		} if ((*p != UCharUnderLine) && !uchareq(t, p)) {
			/*
			 * Not the single-character wildcard and no explicit match? Then
			 * time to quit...
			 */
			return LIKE_FALSE;
		}

		NextChar(t, tlen);
		NextChar(p, plen);
	}

	if (tlen > 0)
		return LIKE_FALSE;      /* end of pattern, but not of text */

	/* End of input string.  Do we have matching pattern remaining? */
	while ((plen > 0) && (*p == UCharPercent))   /* allow multiple %'s at end of
										 		  * pattern */
		NextChar(p, plen);
	if (plen <= 0)
		return LIKE_TRUE;

	/*
	 * End of text with no match, so no point in trying later places to start
	 * matching this pattern.
	 */

	return LIKE_ABORT;
}

/*
 * Shared back-end for mvarchar_like / mvarchar_notlike.  Runs the cached
 * fast path when fn_extra is available, else falls back to MatchUChar.
 */
static bool
do_mvarchar_like(FunctionCallInfo fcinfo, MVarChar *str, MVarChar *pat)
{
	MCharPatternCache  *cache;

	cache = ensure_pattern_cache(fcinfo->flinfo, pat->data,
								 UVARCHARLENGTH(pat), false);
	if (cache != NULL)
		return execute_like_cached(str->data, UVARCHARLENGTH(str), cache);

	/* No fn_extra available (e.g. DirectFunctionCall): legacy path. */
	return MatchUChar(str->data, UVARCHARLENGTH(str),
					  pat->data, UVARCHARLENGTH(pat)) == LIKE_TRUE;
}

PG_FUNCTION_INFO_V1( mvarchar_like );
Datum mvarchar_like( PG_FUNCTION_ARGS );
Datum
mvarchar_like( PG_FUNCTION_ARGS ) {
	MVarChar   *str = PG_GETARG_MVARCHAR(0);
	MVarChar   *pat = PG_GETARG_MVARCHAR(1);
	bool		result = do_mvarchar_like(fcinfo, str, pat);

	PG_FREE_IF_COPY(str,0);
	PG_FREE_IF_COPY(pat,1);

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1( mvarchar_notlike );
Datum mvarchar_notlike( PG_FUNCTION_ARGS );
Datum
mvarchar_notlike( PG_FUNCTION_ARGS ) {
	MVarChar   *str = PG_GETARG_MVARCHAR(0);
	MVarChar   *pat = PG_GETARG_MVARCHAR(1);
	bool		result = do_mvarchar_like(fcinfo, str, pat);

	PG_FREE_IF_COPY(str,0);
	PG_FREE_IF_COPY(pat,1);

	PG_RETURN_BOOL(!result);
}

/*
 * Shared back-end for mchar_like / mchar_notlike.
 */
static bool
do_mchar_like(FunctionCallInfo fcinfo, MChar *str, MVarChar *pat)
{
	MCharPatternCache  *cache;
	UChar			   *filled;
	int					flen;
	bool				result;
	bool				filled_allocated;

	cache = ensure_pattern_cache(fcinfo->flinfo, pat->data,
								 UVARCHARLENGTH(pat), true);
	if (cache == NULL)
	{
		/* No fn_extra: legacy per-call path. */
		bool		isNeedAdd = false;
		UChar	   *cleaned;
		int			clen = 0;
		int			r;

		cleaned = removeTrailingSpaces(pat->data, UVARCHARLENGTH(pat),
									   &clen, &isNeedAdd);
		if (isNeedAdd)
			filled = addTrailingSpace(str, &flen);
		else
		{
			filled = str->data;
			flen = UCHARLENGTH(str);
		}
		r = MatchUChar(filled, flen, cleaned, clen);
		if (pat->data != cleaned)
			pfree(cleaned);
		if (str->data != filled)
			pfree(filled);
		return (r == LIKE_TRUE);
	}

	if (cache->needs_text_pad)
	{
		filled = addTrailingSpace(str, &flen);
		filled_allocated = (filled != str->data);
	}
	else
	{
		filled = str->data;
		flen = UCHARLENGTH(str);
		filled_allocated = false;
	}

	result = execute_like_cached(filled, flen, cache);

	if (filled_allocated)
		pfree(filled);
	return result;
}

PG_FUNCTION_INFO_V1( mchar_like );
Datum mchar_like( PG_FUNCTION_ARGS );
Datum
mchar_like( PG_FUNCTION_ARGS ) {
	MChar	   *str = PG_GETARG_MCHAR(0);
	MVarChar   *pat = PG_GETARG_MVARCHAR(1);
	bool		result = do_mchar_like(fcinfo, str, pat);

	PG_FREE_IF_COPY(str,0);
	PG_FREE_IF_COPY(pat,1);

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1( mchar_notlike );
Datum mchar_notlike( PG_FUNCTION_ARGS );
Datum
mchar_notlike( PG_FUNCTION_ARGS ) {
	MChar	   *str = PG_GETARG_MCHAR(0);
	MVarChar   *pat = PG_GETARG_MVARCHAR(1);
	bool		result = do_mchar_like(fcinfo, str, pat);

	PG_FREE_IF_COPY(str,0);
	PG_FREE_IF_COPY(pat,1);

	PG_RETURN_BOOL(!result);
}



PG_FUNCTION_INFO_V1( mchar_pattern_fixed_prefix );
Datum mchar_pattern_fixed_prefix( PG_FUNCTION_ARGS );
Datum
mchar_pattern_fixed_prefix( PG_FUNCTION_ARGS ) {
	Const			*patt = 	(Const*)PG_GETARG_POINTER(0);
	Pattern_Type	ptype = 	(Pattern_Type)PG_GETARG_INT32(1);
	Const			**prefix = 	(Const**)PG_GETARG_POINTER(2);
	UChar			*spatt;	
	int32			slen, prefixlen=0, restlen=0, i=0;
	MVarChar		*sprefix;
	MVarChar		*srest;
	Pattern_Prefix_Status	status = Pattern_Prefix_None; 

	*prefix = NULL;

	if ( ptype != Pattern_Type_Like )
		PG_RETURN_INT32(Pattern_Prefix_None);

	SET_UCHAR;

	spatt = ((MVarChar*)DatumGetPointer(patt->constvalue))->data;
	slen = UVARCHARLENGTH( DatumGetPointer(patt->constvalue) );

	sprefix = (MVarChar*)palloc( MCHARHDRSZ /*The biggest hdr!! */ + sizeof(UChar) * slen );
	srest = (MVarChar*)palloc( MCHARHDRSZ /*The biggest hdr!! */ + sizeof(UChar) * slen );

	while( prefixlen < slen && i < slen ) {
		if ( spatt[i] == UCharPercent || spatt[i] == UCharUnderLine )
			break;
		else if ( spatt[i] == UCharBackSlesh ) {
			i++;
			if ( i>= slen )
				break;
		}
		sprefix->data[ prefixlen++ ] = spatt[i++];
	}

	while( prefixlen > 0 ) {
		if ( ! u_isspace( sprefix->data[ prefixlen-1 ] ) ) 
			break;
		prefixlen--;
	}

	if ( prefixlen == 0 )
		PG_RETURN_INT32(Pattern_Prefix_None);

	for(;i<slen;i++) 
		srest->data[ restlen++ ] = spatt[i];

	SET_VARSIZE(sprefix, sizeof(UChar) * prefixlen + MVARCHARHDRSZ);	
	SET_VARSIZE(srest, sizeof(UChar) * restlen + MVARCHARHDRSZ);	

	*prefix = makeConst( patt->consttype, -1, InvalidOid, VARSIZE(sprefix), PointerGetDatum(sprefix), false, false );

	if ( prefixlen == slen )	/* in LIKE, an empty pattern is an exact match! */
		status = Pattern_Prefix_Exact;
	else if ( prefixlen > 0 )
		status = Pattern_Prefix_Partial;

	PG_RETURN_INT32( status );	
}

static bool 
checkCmp( UChar *left, int32 leftlen, UChar *right, int32 rightlen ) {

	return  (UCharCaseCompare( left, leftlen, right, rightlen) < 0 ) ? true : false;
}


PG_FUNCTION_INFO_V1( mchar_greaterstring );
Datum mchar_greaterstring( PG_FUNCTION_ARGS );
Datum
mchar_greaterstring( PG_FUNCTION_ARGS ) {
	Const			*patt = 	(Const*)PG_GETARG_POINTER(0);
	char			*src  = 	(char*)DatumGetPointer( patt->constvalue ); 
	int				dstlen, srclen  = 	VARSIZE(src);
	char 			*dst = palloc( srclen );
	UChar			*ptr, *srcptr;

	memcpy( dst, src, srclen );

	srclen = dstlen = UVARCHARLENGTH( dst );
	ptr    = ((MVarChar*)dst)->data;
	srcptr    = ((MVarChar*)src)->data;

	while( dstlen > 0 ) {
		UChar	*lastchar = ptr + dstlen - 1;

		if ( !U16_IS_LEAD( *lastchar ) ) {
			while( *lastchar<0xffff ) {

				(*lastchar)++;

				if ( ublock_getCode(*lastchar) == UBLOCK_INVALID_CODE || !checkCmp( srcptr, srclen, ptr, dstlen ) )
					continue;
				else {
					SET_VARSIZE(dst, sizeof(UChar) * dstlen + MVARCHARHDRSZ);
				
					PG_RETURN_POINTER( makeConst( patt->consttype, -1,
												  InvalidOid, VARSIZE(dst), PointerGetDatum(dst), false, false ) );
				}
			}
		}
				
		dstlen--;
	}

	PG_RETURN_POINTER(NULL);
}

static int 
do_like_escape( UChar *pat, int plen, UChar *esc, int elen, UChar *result) {
	UChar	*p = pat,*e =esc ,*r;
	bool	afterescape;

	r = result;
	SET_UCHAR;

	if ( elen == 0 ) {
		/*
		 * No escape character is wanted.  Double any backslashes in the
		 * pattern to make them act like ordinary characters.
		 */
		while (plen > 0) {
			if (*p == UCharBackSlesh ) 
				*r++ = UCharBackSlesh;
			CopyAdvChar(r, p, plen);
		}
	} else {
		/*
		 * The specified escape must be only a single character.
		 */
		NextChar(e, elen);

		if (elen != 0)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
				errmsg("invalid escape string"),
				errhint("Escape string must be empty or one character.")));

		e = esc;

		/*
		 * If specified escape is '\', just copy the pattern as-is.
		 */
		if ( *e == UCharBackSlesh ) {
			memcpy(result, pat, plen * sizeof(UChar));
			return plen;
		}

		/*
		 * Otherwise, convert occurrences of the specified escape character to
		 * '\', and double occurrences of '\' --- unless they immediately
		 * follow an escape character!
		 */
		afterescape = false;

		while (plen > 0) {
			if ( uchareq(p,e) && !afterescape) {
				*r++ = UCharBackSlesh;
				NextChar(p, plen);
				afterescape = true;
			} else if ( *p == UCharBackSlesh ) {
				*r++ = UCharBackSlesh;
				if (!afterescape)
					*r++ = UCharBackSlesh;
				NextChar(p, plen);
				afterescape = false;
			} else {
				CopyAdvChar(r, p, plen);
				afterescape = false;
			}
		}
	}

	return  ( r -  result );
}

PG_FUNCTION_INFO_V1( mvarchar_like_escape );
Datum mvarchar_like_escape( PG_FUNCTION_ARGS );
Datum
mvarchar_like_escape( PG_FUNCTION_ARGS ) {
	MVarChar	*pat = PG_GETARG_MVARCHAR(0);
	MVarChar	*esc = PG_GETARG_MVARCHAR(1);
	MVarChar	*result;

	result = (MVarChar*)palloc( MVARCHARHDRSZ + sizeof(UChar)*2*UVARCHARLENGTH(pat) );
	result->len = MVARCHARHDRSZ + do_like_escape( pat->data, UVARCHARLENGTH(pat),
							 	  				  esc->data, UVARCHARLENGTH(esc),
								  				  result->data ) * sizeof(UChar);

	SET_VARSIZE(result, result->len);
	PG_FREE_IF_COPY(pat,0);
	PG_FREE_IF_COPY(esc,1);

	PG_RETURN_MVARCHAR(result);
}

static MemoryContext McharRgCntx;

#define RE_CACHE_SIZE	32
typedef struct ReCache {
	MemoryContext	cntx;
	UChar	*pattern;
	int		length;
	int		flags;
	regex_t	re;
} ReCache;

static int  num_res = 0;
static ReCache re_array[RE_CACHE_SIZE];  /* cached re's */
static const int mchar_regex_flavor = REG_ADVANCED | REG_ICASE;

static regex_t *
URE_compile_and_cache(UChar *text_re, int text_re_len, int cflags) {
	pg_wchar	*pattern;
	size_t		pattern_len;
	int			i;
	int			regcomp_result;
	ReCache		re_temp;
	char		errMsg[128];
	MemoryContext	oldcntx;
	char*			patternId;


	for (i = 0; i < num_res; i++) {
		if ( re_array[i].length == text_re_len &&
			 re_array[i].flags == cflags &&
			 memcmp(re_array[i].pattern, text_re, sizeof(UChar)*text_re_len) == 0 ) {

			 /* Found, move it to front */
			 if ( i>0 ) {
				re_temp = re_array[i];
				memmove(&re_array[1], &re_array[0], i * sizeof(ReCache));
				re_array[0] = re_temp;
			}

			return &re_array[0].re;
		}
	}

	if (McharRgCntx == NULL)
		McharRgCntx = AllocSetContextCreate(TopMemoryContext,
											"McharRgCntx",
											ALLOCSET_SMALL_SIZES);

	pattern = (pg_wchar *) palloc((1 + text_re_len) * sizeof(pg_wchar));
	pattern_len =  UChar2Wchar(text_re, text_re_len, pattern);

	re_temp.cntx = AllocSetContextCreate(CurrentMemoryContext,
										 "McharRegex",
										 ALLOCSET_SMALL_SIZES);

	oldcntx = MemoryContextSwitchTo(re_temp.cntx);

	regcomp_result = pg_regcomp(&re_temp.re,
								pattern,
								pattern_len,
								cflags,
								DEFAULT_COLLATION_OID);
	pfree( pattern );

	if (regcomp_result != REG_OKAY) {
		pg_regerror(regcomp_result, &re_temp.re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				errmsg("invalid regular expression: %s", errMsg)));
	}

	re_temp.pattern = palloc(text_re_len * sizeof(UChar));
	memcpy(re_temp.pattern, text_re, text_re_len*sizeof(UChar));
	re_temp.length = text_re_len;
	re_temp.flags = cflags;

	patternId = palloc0(text_re_len * sizeof(UChar) + 1);
	UChar2Char(re_temp.pattern, text_re_len, patternId);
	MemoryContextSetIdentifier(re_temp.cntx, patternId);

	if (num_res >= RE_CACHE_SIZE) {
		--num_res;
		Assert(num_res < RE_CACHE_SIZE);
		MemoryContextDelete(re_array[num_res].cntx);
	}

	MemoryContextSetParent(re_temp.cntx, McharRgCntx);

	if (num_res > 0)
		memmove(&re_array[1], &re_array[0], num_res * sizeof(ReCache));

	re_array[0] = re_temp;
	num_res++;

	MemoryContextSwitchTo(oldcntx);

	return &re_array[0].re;
}

static bool
URE_compile_and_execute(UChar *pat, int pat_len, UChar *dat, int dat_len,
						int cflags, int nmatch, regmatch_t *pmatch) {
	pg_wchar   *data;
	size_t      data_len;
	int         regexec_result;
	regex_t    *re;
	char        errMsg[128];

	data = (pg_wchar *) palloc((1+dat_len) * sizeof(pg_wchar));
	data_len = UChar2Wchar(dat, dat_len, data);

	re = URE_compile_and_cache(pat, pat_len, cflags);

	regexec_result = pg_regexec(re,
								data,
								data_len,
								0,
								NULL,
								nmatch,
								pmatch,
								0);
	pfree(data);

	if (regexec_result != REG_OKAY && regexec_result != REG_NOMATCH) {
		/* re failed??? */
		pg_regerror(regexec_result, re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				errmsg("regular expression failed: %s", errMsg)));
	}

	return (regexec_result == REG_OKAY);
}

PG_FUNCTION_INFO_V1( mchar_regexeq );
Datum mchar_regexeq( PG_FUNCTION_ARGS );
Datum
mchar_regexeq( PG_FUNCTION_ARGS ) {
	MChar	*t = PG_GETARG_MCHAR(0);
	MChar	*p = PG_GETARG_MCHAR(1);
	bool 	res;

	res = URE_compile_and_execute(p->data, UCHARLENGTH(p),
								 t->data, UCHARLENGTH(t),
								 mchar_regex_flavor,
								 0, NULL);
	PG_FREE_IF_COPY(t, 0);
	PG_FREE_IF_COPY(p, 1);

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1( mchar_regexne );
Datum mchar_regexne( PG_FUNCTION_ARGS );
Datum
mchar_regexne( PG_FUNCTION_ARGS ) {
	MChar	*t = PG_GETARG_MCHAR(0);
	MChar	*p = PG_GETARG_MCHAR(1);
	bool 	res;

	res = URE_compile_and_execute(p->data, UCHARLENGTH(p),
								 t->data, UCHARLENGTH(t),
								 mchar_regex_flavor,
								 0, NULL);
	PG_FREE_IF_COPY(t, 0);
	PG_FREE_IF_COPY(p, 1);

	PG_RETURN_BOOL(!res);
}

PG_FUNCTION_INFO_V1( mvarchar_regexeq );
Datum mvarchar_regexeq( PG_FUNCTION_ARGS );
Datum
mvarchar_regexeq( PG_FUNCTION_ARGS ) {
	MVarChar	*t = PG_GETARG_MVARCHAR(0);
	MVarChar	*p = PG_GETARG_MVARCHAR(1);
	bool 	res;

	res = URE_compile_and_execute(p->data, UVARCHARLENGTH(p),
								 t->data, UVARCHARLENGTH(t),
								 mchar_regex_flavor,
								 0, NULL);
	PG_FREE_IF_COPY(t, 0);
	PG_FREE_IF_COPY(p, 1);

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1( mvarchar_regexne );
Datum mvarchar_regexne( PG_FUNCTION_ARGS );
Datum
mvarchar_regexne( PG_FUNCTION_ARGS ) {
	MVarChar	*t = PG_GETARG_MVARCHAR(0);
	MVarChar	*p = PG_GETARG_MVARCHAR(1);
	bool 	res;

	res = URE_compile_and_execute(p->data, UVARCHARLENGTH(p),
								 t->data, UVARCHARLENGTH(t),
								 mchar_regex_flavor,
								 0, NULL);
	PG_FREE_IF_COPY(t, 0);
	PG_FREE_IF_COPY(p, 1);

	PG_RETURN_BOOL(!res);
}

static int
do_similar_escape(UChar *p, int plen, UChar *e, int elen, UChar *result) {
	UChar	*r;
	bool	afterescape = false;
	bool	incharclass = false;
	int		nquotes = 0;

	SET_UCHAR;

	if (e==NULL || elen <0 ) {
		e = &UCharBackSlesh;
		elen = 1;
	} else {
		if ( elen == 0 )
			e = NULL;
		else if ( elen != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
					errmsg("invalid escape string"),
					errhint("Escape string must be empty or one character.")));
	}

	/*
	 * Look explanation of following in ./utils/adt/regexp.c 
	 */
	r = result;

	*r++ = UCharUp;
	*r++ = UCharLBracket;
	*r++ = UCharQ;
	*r++ = UCharDotDot;

	while( plen>0 ) {
		UChar	pchar = *p;

		if (afterescape)
		{
			if (pchar == UCharQuote && !incharclass) /* escape-double-quote? */
			{
				if (nquotes == 0)
				{
					*r++ = UCharRBracket;
					*r++ = UCharLFBracket;
					*r++ = UCharOne;
					*r++ = UCharComma;
					*r++ = UCharOne;
					*r++ = UCharRFBracket;
					*r++ = UCharQ;
					*r++ = UCharLBracket;
				}
				else if (nquotes == 1)
				{
					*r++ = UCharRBracket;
					*r++ = UCharLFBracket;
					*r++ = UCharOne;
					*r++ = UCharComma;
					*r++ = UCharOne;
					*r++ = UCharRFBracket;
					*r++ = UCharLBracket;
					*r++ = UCharQ;
					*r++ = UCharDotDot;
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER),
							 errmsg("SQL regular expression may not contain more than two escape-double-quote separators")));
				nquotes++;
			}
			else
			{
				*r++ = UCharBackSlesh;
				*r++ = pchar;
			}
			afterescape = false;
		}
		else if (e && elen > 0 && pchar == *e)
		{
			afterescape = true;
		}
		else if (incharclass)
		{
			if (pchar == UCharBackSlesh)
				*r++ = UCharBackSlesh;
			*r++ = pchar;
			if (pchar == UCharRQBracket)
				incharclass = false;
		}
		else if (pchar == UCharLQBracket)
		{
			*r++ = pchar;
			incharclass = true;
		}
		else if (pchar == UCharPercent)
		{
			*r++ = UCharDot;
			*r++ = UCharStar;
		}
		else if (pchar == UCharUnderLine)
			*r++ = UCharDot;
		else if (pchar == UCharLBracket)
		{
			*r++ = UCharLBracket;
			*r++ = UCharQ;
			*r++ = UCharDotDot;
		}
		else if (pchar == UCharBackSlesh || pchar == UCharDot ||
				 pchar == UCharUp || pchar == UCharDollar)
		{
			*r++ = UCharBackSlesh;
			*r++ = pchar;
		}
		else
			*r++ = pchar;

		 p++, plen--;
	}

	*r++ = UCharRBracket;
	*r++ = UCharDollar;

	return r-result;
}

PG_FUNCTION_INFO_V1( mchar_similar_escape );
Datum mchar_similar_escape( PG_FUNCTION_ARGS );
Datum
mchar_similar_escape( PG_FUNCTION_ARGS ) {
	MChar	*pat;
	MChar	*esc;
	MChar	*result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pat = PG_GETARG_MCHAR(0);

	if (PG_NARGS() < 2 || PG_ARGISNULL(1)) {
		esc = NULL;
	} else {
		esc = PG_GETARG_MCHAR(1);
	}

	result = (MChar*)palloc( MCHARHDRSZ + sizeof(UChar)*(23 + 3*UCHARLENGTH(pat)) );
	result->len = MCHARHDRSZ + do_similar_escape( pat->data, UCHARLENGTH(pat),
							 	  (esc) ? esc->data : NULL, (esc) ? UCHARLENGTH(esc) : -1,
								  result->data ) * sizeof(UChar);
	result->typmod=-1;

	SET_VARSIZE(result, result->len);
	PG_FREE_IF_COPY(pat,0);
	if ( esc )
		PG_FREE_IF_COPY(esc,1);

	PG_RETURN_MCHAR(result);
}

PG_FUNCTION_INFO_V1( mvarchar_similar_escape );
Datum mvarchar_similar_escape( PG_FUNCTION_ARGS );
Datum
mvarchar_similar_escape( PG_FUNCTION_ARGS ) {
	MVarChar	*pat;
	MVarChar	*esc;
	MVarChar	*result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pat = PG_GETARG_MVARCHAR(0);

	if (PG_NARGS() < 2 || PG_ARGISNULL(1)) {
		esc = NULL;
	} else {
		esc = PG_GETARG_MVARCHAR(1);
	}

	result = (MVarChar*)palloc( MVARCHARHDRSZ + sizeof(UChar)*(23 + 3*UVARCHARLENGTH(pat)) );
	result->len = MVARCHARHDRSZ + do_similar_escape( pat->data, UVARCHARLENGTH(pat),
							 	  				(esc) ? esc->data : NULL, (esc) ? UVARCHARLENGTH(esc) : -1,
								  				  result->data ) * sizeof(UChar);

	SET_VARSIZE(result, result->len);
	PG_FREE_IF_COPY(pat,0);
	if ( esc )
		PG_FREE_IF_COPY(esc,1);

	PG_RETURN_MVARCHAR(result);
}

#define RE_CACHE_SIZE	32
