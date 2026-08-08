# Отчёт о сверке драфта `why-numeric-is-slow.md` с первоисточниками

Дата: 8 августа 2026. Все страницы открыты и прочитаны, все коммиты проверены по
полному клону `postgres.git`, ассемблер — собран и посмотрен глазами (gcc 13.3, `-O2`).

Легенда: **[ОШИБКА]** — было неверно, исправлено. **[НЕТОЧНО]** — было близко к правде,
но пересказ вводил в заблуждение. **[ПОДТВЕРЖДЕНО]** — сверено, оставлено как было.
**[ДОБАВЛЕНО]** — новый факт, которого в драфте не было.

---

## 1. Самое важное: приоритет по патчу

**[ДОБАВЛЕНО, критично]** Патч из §1.1 HANDOFF — не первая попытка, и предыдущую
**отклонили**.

- **Chenhui Mo**, [«Optimize numeric comparisons and aggregations via packed-datum
  extraction»](https://commitfest.postgresql.org/patch/6651/), 4–5 апреля 2026.
  Формулировка проблемы дословно совпадает: «PG_GETARG_NUMERIC() unconditionally
  detoasts short-header datums, which incurs continuous palloc/memcpy overhead in
  tight aggregation loops». Заявлено 23–70 % на 20 млн строк.
  **Статус: Rejected**, коммитфест PG20-1, закрыт 10 мая 2026. Формальных ревью — ноль.
  В v1 был SIGBUS на платформах со строгим выравниванием (чтение `int16` по нечётному
  смещению), в v2 исправлено буфером на стеке.
- Продолжение того же автора — [«Make NumericVar storage semantics explicit»](https://commitfest.postgresql.org/patch/6751/),
  Withdrawn 26 июля 2026 после «Waiting on Author» от Джона Нейлора.
- **Грег Старк** предлагал ровно эту идею ещё в ноябре 2015, в треде «LLVM miscompiles
  numeric.c access to short numeric var headers». Не реализована.
- **Эндрю Гирт** уже реализовал этот приём в ядре — коммит `abd94bcac4`, PostgreSQL 9.5,
  функция `numeric_abbrev_convert()`, комментарий: «This is to handle packed datums
  without needing a palloc/pfree cycle». Но только в sortsupport.
- Аналог для `text`/`bytea` — Том Лейн, коммит `1b393f4e5d`, PostgreSQL 9.1.

**Практический вывод для письма в hackers:** в текущем master `numeric_cmp`, `numeric_eq`
и `hash_numeric` действительно всё ещё вызывают голый `PG_GETARG_NUMERIC` (строки 2423,
2439, 2721) — приём не реализован. Но ссылаться надо на все четыре пункта выше,
и обязательно закрыть выравнивание и принести замеры на запросах, а не на микробенчмарках.

---

## 2. Хронология pgsql-hackers

| # | Было в драфте | Проверка |
|---|---|---|
| 2012 | коммит без автора и хеша | **[ДОБАВЛЕНО]** `5cb0e335976befdcedd069c59dd3858fb3e649b3`, патч **Kyotaro Horiguchi**, коммитил **Heikki Linnakangas**, 21.11.2012, **PostgreSQL 9.3**. **[ОШИБКА в трактовке]** Патч касается арифметики/вывода (`numeric_add/sub/mul/div`, `numeric_out`, `numeric_int8`), **не** сравнения и хэша — `cmp_numerics()` вообще не использует `NumericVar`. И `init_var_from_num()` — это не «буфер на стеке», а zero-copy алиас: `digits` указывает внутрь исходного значения. Detoast-копию из `PG_GETARG_NUMERIC` он не убирает. |
| 2013 IEEE | «обсуждение добавления» | **[ДОБАВЛЕНО]** Автор — **Craig Ringer**, 11.06.2013. Закрыто Томом Лейном в тот же день, патча не было вовсе. |
| 2013 avg | ссылка на письмо | **[НЕТОЧНО]** Указанный URL — административная реплика Джоша Беркуса, а не начало треда. Числа «25 % / 50 %» — Павла Стехуле. Результат треда: коммит `69c8fbac20`, патч **Hadi Moshayedi**, коммитил Том Лейн, **PostgreSQL 9.4**. |
| 2015 int128 | «Принят патч» | **[ОШИБКА]** Автор — **Andreas Karlsson**, не Андрес Фройнд (он коммитил). Коммиты `8122e1437e` + `959277a4f5`, 20.03.2015, **PostgreSQL 9.5**. **[ДОБАВЛЕНО]** Цифры из письма Карлссона: `sum(int8)` 2521 → 1023 мс, `var_samp(int4)` 3809 → 1033 мс на 10 млн строк. |
| 2024 «25–81 %» | приписано треду Rasheed | **[ОШИБКА]** Фраза «Impressive speed-up, between 25% - 81%» — **Joel Jacobson**, 05.07.2024, письмо `ce08a807-b3ca-4316-8fcf-98be5dec10a2@app.fastmail.com`. Указанный в драфте URL (письмо Рашида от 01.07.2024) **не содержит процентов вообще**, только миллисекунды. И относится цифра к **промежуточной v7** патча `mul_var_small`, к **микробенчмарку `numeric_mul()`** на операндах 1–4 базовых цифры, на трёх CPU. На уровне запроса тот же автор получил 11–13 %. |
| 2024 коммиты | не названы | **[ДОБАВЛЕНО]** `ca481d3c9a`, `c4e44224c`, `8dc28d7eb8` (умножение), `9428c001f6` (деление), все Dean Rasheed, все **PostgreSQL 18**. Строка в release notes дословно: «Improve the speed of numeric multiplication and division (Joel Jacobson, Dean Rasheed)». `8dc28d7eb8` обещает 3–6× на длинных входах и предупреждает о **замедлении на 32-битных** машинах. |
| 2025 int128.h | «расширяют» | **[ОШИБКА]** Закоммичено: пять коммитов Dean Rasheed, август 2025, ключевой `d699687b32`, **PostgreSQL 19** (не 18). И это не про арифметику numeric, а про int128-аккумуляторы для **целочисленных** агрегатов на платформах **без** родного `__int128`, плюс упрощение кода. В release notes PG 19 отдельной строки нет. Указанный в драфте message-id — поздняя реплика; в коммитах как `Discussion:` стоит `CAEZATCWgBMc9ZwKMYqQpaQz2X6gaamYRB+RnMsUNcdMcL2Mj_w@mail.gmail.com`. |

**[ДОБАВЛЕНО]** Свежих 2025–2026 тредов про производительность numeric, кроме патчей
Chenhui Mo, нет. `git log` по `numeric.c` за 2026 год — только правки комментариев,
приведение типов и copyright.

---

## 3. Утверждения по исходникам PostgreSQL — все проверены локально

**[ПОДТВЕРЖДЕНО]**

- `NUMERIC_MIN_SIG_DIGITS = 16` (`src/include/utils/numeric.h:53`), комментарий к
  `select_div_scale()` про «no less accurate than float8» — дословно как в драфте.
  Арифметика примеров с делением сходится: `1/3` → rscale 20, `1000000/3` → 12.
- `NumericSumAccum`: 32-битные ячейки, `pos_digits`/`neg_digits` раздельно, перенос
  раз в `NBASE - 1` = **9999** значений (`numeric.c:12049`).
- `hash_numeric`: снимает ведущие и хвостовые нули, для нуля возвращает
  `PG_RETURN_UINT32(-1)`, для NaN/Inf — 0.
- `numeric_abbrev_abort`: HyperLogLog, порог — меньше одного различного значения
  на 10 000 входов, плюс отдельный выход при >100k различных.
- **Хэш действительно лежит на диске.** `JsonbHashScalarValue()` в `jsonb_util.c`
  вызывает `hash_numeric` через `DirectFunctionCall1`, а `gin_extract_jsonb_path`
  сохраняет результат. Утверждение про `jsonb_path_ops` верно.
- Кэш смещений: `TupleDescFinalize()` в `tupdesc.c` рвёт цепочку на `attlen <= 0` —
  это ровно numeric.

**[ОШИБКА]**

- «Исходник `heaptuple.c` — как строка разбирается на колонки» — `slot_deform_heap_tuple()`
  живёт в **`src/backend/executor/execTuples.c`**, а правило обрыва кэша — в
  `src/backend/access/common/tupdesc.c`. В `heaptuple.c` только `heap_deform_tuple()`
  и `nocachegetattr()`.

**[НЕТОЧНО]**

- «Короткий заголовок применяется, когда значение помещается в 126 байт» — 126 байт
  это **полезная нагрузка**, всего с заголовком 127 (`VARATT_SHORT_MAX = 0x7F`,
  `VARATT_CAN_MAKE_SHORT` в `varatt.h`).

---

## 4. decimal128 и IEEE 754

| Утверждение | Вердикт |
|---|---|
| 34 цифры, 16 байт | **[ПОДТВЕРЖДЕНО]** |
| экспонента −6143…+6144 | **[НЕТОЧНО]** Верно для `emin…emax`. Но хранится смещённая экспонента 0…12287 (bias 6176), а квантовая экспонента q идёт −6176…+6111. В тексте теперь уточнено. Наибольшее конечное значение — 9,999…×10⁶¹⁴⁴, а не 10⁶¹⁴⁴. |
| у нуля 12288 представлений | **[ПОДТВЕРЖДЕНО]** Проверено тремя способами: диапазон q (6111 − (−6176) + 1), кодировка (3 × 2¹²), смещение (0…12287). |
| когорты, preferred exponent, `quantize` | **[ПОДТВЕРЖДЕНО]** по тексту стандарта и по speleotrove |
| BID и DPD | **[ПОДТВЕРЖДЕНО]** |
| нет 128÷128 на x86-64, `divq` = 128÷64 с #DE | **[ПОДТВЕРЖДЕНО]** |
| «общий случай → `__udivti3`» | **[НЕТОЧНО]** Слишком сильно. Проверено ассемблером: `a / b` с переменным делителем → `call __udivti3@PLT`, но `a / 10` и `a / 100` инлайнятся в умножение на обратную величину, `a / 1024` — в сдвиг. `a / 10^19` снова уходит в libgcc. Умножение инлайнится всегда (три `imulq`/`mulq`). Для numeric это существенно: деление на константный NBASE дёшево. |
| DFP-железо только у IBM | **[НЕПОЛНО]** Плюс **Fujitsu SPARC64 X/X+** — вендор это документирует. У x86 не было никогда ни у одного производителя; Intel предлагает программную библиотеку в кодировке BID. |
| numeric: 131072 / 16383 цифр | **[ПОДТВЕРЖДЕНО]** дословно. Добавлено: в `NUMERIC(p,s)` объявить можно не больше 1000. |
| цитата «If you require exact storage…» | **[ПОДТВЕРЖДЕНО]** дословно, но лежит в §8.1.3 «Floating-Point Types», а не в разделе про numeric. Исправлено. |

### pgdecimal2 — самая крупная правка в этом разделе

**[ОШИБКА в атрибуции и в цифрах]** «По замерам автора decimal64 вдвое быстрее numeric,
decimal128 — в полтора раза».

- Автор этих слов — **Feng Tian** (Vitesse Data), письмо от 24.09.2015. Не Павел Стехуле,
  который на PGXN числится автором дистрибутива по унаследованным метаданным.
- Фраза встречается **ровно один раз** — в письме. Ни на PGXN, ни в README её нет.
- Опубликованные им же тайминги в README дают **1,62× и 1,27×**, а не 2× и 1,5×.
- Мерялось против **`numeric(15,3)`** (ограниченный тип), на Core i5 5252U 1,6 ГГц,
  на сборке **Vitesse DB** с `set vitesse.enable = 0`, а не на ванильном PostgreSQL.
  Версия PostgreSQL нигде не указана. Дэвид Роули спрашивал про железо — ответа нет.
- **[ДОБАВЛЕНО]** Реальная причина, по которой расширение никуда не пошло, названа
  самим автором в треде: decNumber «is either GPL, or ICU license».
- **[ДОБАВЛЕНО]** Замечание Томаса Манро (2017): DECFLOAT попал в стандарт SQL как
  фича T076; decimal32/64 можно было бы передавать по значению, decimal128 — нет,
  и механизма «один тип, две дисциплины передачи» в PostgreSQL нет. Это ровно
  тема статьи, стоит цитировать.

---

## 5. Раздел «А как у соседей?»

| Источник | Вердикт |
|---|---|
| DuckDB, цитата про width > 19 | **[НЕТОЧНО]** Цитата была обрезана с двух сторон. Восстановлена полностью, включая «unless there is a good reason for why this is insufficient». Таблица 2/4/8/16 байт и диапазоны 1–4/5–9/10–18/19–38 — подтверждены. |
| ClickHouse, эмуляция 128/256 | **[ПОДТВЕРЖДЕНО]** дословно. Но байтовых размеров на странице Decimal **нет** — они на странице формата RowBinary. Ссылка поправлена на канонический `/docs/sql-reference/data-types/decimal`. |
| Altinity про Decimal vs Float | **[ОШИБКА]** Замеров там **нет вообще** — это концептуальный пост Мацея Бонка (2024), пересказывающий тезис про эмуляцию. Ссылка убрана из статьи. |
| bornsql: `decimal(15,2)` = 9 байт | **[ОШИБКА в цитировании]** В той статье про `decimal(15,2)` нет ни слова, её пример — `decimal(19,4)`. Само число верное. Ссылка заменена на таблицу Microsoft. |
| bornsql «Data Efficiency: DECIMAL» | **[ПОДТВЕРЖДЕНО]** Дословно: «A precision of 10–19 digits requires 9 bytes (a 45 % jump in storage required), and this is the sweet spot where a BIGINT is an effective alternative». Автор — Randolph West. |
| mssqltips 3323, 150 млн строк | **[ОШИБКА]** Конкретных чисел в тексте **нет** — они только внутри картинок. Единственные количественные сравнения в тексте («60 % больше», «на треть дольше») относятся к BIGINT против INT, а про DECIMAL сказано лишь «larger still» / «substantially worse». Ссылка убрана. |
| mssqltips 5826, CPU float vs numeric | **[ПОДТВЕРЖДЕНО и раскрыто]** 499 999 строк, `numeric(19,8)` против `float`: все операции вместе **1062 мс против 391 мс CPU**, деление 547 против 296. Вывод автора: «anywhere from 35–45 % less». Числа теперь в статье. |
| SQL Server MS-TDS | **[ПОДТВЕРЖДЕНО]** дословно: байт знака + целое 4/8/12/16 байт → 5/9/13/17. |
| SQL Server `money` | **[ПОДТВЕРЖДЕНО]** 8 байт, точность до одной десятитысячной. |
| MySQL: 9 цифр в 4 байта | **[ПОДТВЕРЖДЕНО]** дословно из мануала 8.4. |
| MySQL: до 5.0.3 строкой | **[ПОДТВЕРЖДЕНО]** Ссылка на зеркало `documentation.help` заменена на официальный мануал 5.0, размещённый у Oracle. |
| Oracle NUMBER, «до 21 байта» | **[НЕТОЧНО]** 1 байт экспоненты + до 20 байт мантиссы = 21 байт значения; SQL Reference пишет «from 1 to 22 bytes», считая ещё байт длины. Уточнено. |
| Oracle: ссылка на Data-Types про производительность | **[ОШИБКА]** На той странице никакого утверждения о производительности нет, только «more robust». Заменено на Database Concepts: «binary precision, which enables faster arithmetic calculations». |
| oracle-base, Тим Холл | **[ПОДТВЕРЖДЕНО и раскрыто]** 10 млн итераций: NUMBER 26 сотых секунды, BINARY_DOUBLE 15, BINARY_FLOAT 14, PLS_INTEGER 9. Важная оговорка добавлена: это **только PL/SQL**, не SQL и не хранение. Бонус: `INTEGER` — 44 сотых, то есть медленнее самого NUMBER. |
| Db2 DECFLOAT = decimal64/128 | **[НЕТОЧНО]** IBM пишет «IEEE 754r number» и имён decimal64/decimal128 не употребляет, байтовых размеров не даёт. Эквивалентность доказывается совпадением параметров (16/34 цифры, Emax 384/6144, Emin −383/−6143) — так теперь и написано. |
| POWER6 / мейнфреймы z | **[ОШИБКА в модели]** У **z9** DFP была реализована «with a mixture of low-level software and hardware assists», настоящий аппаратный блок — только в **z10**, и он выведен из POWER6-шного. Ссылка на блог SAP заменена на статью Schwarz, Kapernick & Cowlishaw в IBM Journal of Research and Development (январь 2009). |
| speleotrove.com | **[ПОДТВЕРЖДЕНО]** Сайт Майка Каулишо, оттуда же decNumber. |

---

## 6. Что добавлено нового

**История типа** (по просьбе — целый новый раздел):

- Коммит `0e9d75c6ac`, **Jan Wieck, 30 декабря 1998**, вышел в **PostgreSQL 6.5**.
  В `pg_type.h` сразу `typlen = -1` и описание «arbitrary precision exact numeric data type».
- До этого: в Postgres95 десятичного типа нет вообще, в 6.4 `NUMERIC`/`DECIMAL` —
  парсерная имитация поверх `int4` с проверкой «precision must be 9, scale must be zero».
- Мотив — [письмо Вика от 18.12.1998](https://www.postgresql.org/message-id/m0zr4z8-000EBPC%40orion.SAPserv.Hamburg.dsh.de),
  ответ на TODO «Add full ANSI SQL capabilities»: стандартная семантика `NUMERIC[(p[,s])]`,
  «exact representation of arbitrary precise numbers», с оглядкой на чужие пределы
  «from 38 (Oracle) to over 1000».
- Цену понимали сразу — Локхарт на следующий день: «you would hate to take the hit
  representing everything as extended precision even if the actual range is int4/float8».
- **Ключевое архитектурное решение — [Том Лейн, 13 апреля 2001](https://www.postgresql.org/message-id/3077.987145046%40sss.pgh.pa.us)**,
  в ответ на предложение сделать numeric фиксированной длины: «I rather like a numeric
  type that can handle ranges wider than double precision… I do object to emasculating
  the type we have». Там же он предлагает base-10000 как правильную альтернативу.
- Base-10000 — коммит `d72f6c7503`, Том Лейн, 2003, **PostgreSQL 7.4**, «about a factor
  of ten speedup on the 'numeric' regression test». Идея, однако, [не его, а Вика](https://www.postgresql.org/message-id/m10ctKO-000EBPC%40orion.SAPserv.Hamburg.dsh.de),
  апрель 1999. Тем же коммитом из заголовка выкинут `n_rscale` (10 → 8 байт) и
  `NUMERIC_MAX_PRECISION` урезан с 4000 до 1000 («must be small enough that dscale
  values will fit in 14 bits»).
- Короткий заголовок — коммит `145343534c`, **Robert Haas, 2010, PostgreSQL 9.1**
  (не 8.4 и не 9.0). Подготовка — `f828f878e9`, Том Лейн, 8.3, перестановка полей
  специально ради будущего сжатия.
- `NaN` — с первого коммита 1998 года. `±Infinity` — коммит `a57d312a77`, Том Лейн,
  июль 2020, **PostgreSQL 14**.
- Эволюция накладных расходов: 10 байт (6.5) → 8 (7.4) → 5 (8.3, короткий varlena) →
  3 (9.1, `NumericShort`).

**Живые свидетельства вместо одних доков** (раньше их в статье не было совсем):

- [Cybertec, Kaarel Moppel, 2017](https://www.cybertec-postgresql.com/en/int4-vs-int8-vs-uuid-vs-numeric-performance-on-bigger-joins/):
  джойны, 5 млн строк — int4 2,72 с, numeric 3,65 с (+34 %), **при одинаковом размере
  индекса 107 МБ**. Лучшее доказательство тезиса «дело в CPU, а не в объёме».
- [Утечка памяти в memoize по numeric-ключу](https://www.postgresql.org/message-id/83281eed63c74e4f940317186372abfd@cft.ru),
  Алексей Орлов, ЦФТ, 2023: 600k строк, `ExecutorState` 24 075 240 → 74 840 байт.
  Названа точная цепочка `MemoizeHash_hash() → … → hash_numeric() → pg_detoast_datum() → palloc()`.
- [BUG #12675](https://www.postgresql.org/message-id/54C778BF.3000205%40vmware.com),
  Хейкки Линнаканген, 2015: почему `double precision` быстрее `bigint` — «arithmetic
  on numeric is much slower than on double».
- [Лукас Фиттл, pganalyze, 2022](https://pganalyze.com/blog/5mins-postgres-large-integers-causing-sequential-scan-instead-of-using-index):
  большой целочисленный литерал парсится как numeric, приводит колонку, убивает индекс —
  0,1 мс → 100 мс.
- [Том Лейн, 2007](https://www.postgresql.org/message-id/19101.1180967738%40sss.pgh.pa.us):
  почему `sum()` — негодный бенчмарк для сравнения типов, «100 % faulty».
- Работа Дэвида Роули по deform: `d28dff3f` (`CompactAttribute`, ~10 % TPS, до 25 % на
  Zen4, PostgreSQL 18) и [«More speedups for tuple deformation»](https://www.postgresql.org/message-id/CAApHDvpoFjaj3+w_jD5uPnGazaw41A71tVJokLDJg2zfcigpMQ@mail.gmail.com)
  (в среднем 21 %, PostgreSQL 19). Половина тестовых случаев там отличается ровно тем,
  `INT` или `TEXT` стоит первой колонкой.
- [Kyotaro Horiguchi, 2012](https://www.postgresql.org/message-id/20120914.172508.259995810.horiguchi.kyotaro@lab.ntt.co.jp):
  базовая линия `sum()` по 10 млн строк — int 1570 мс, numeric 3930 мс.

---

## 7. Что осталось непроверенным

- Точная пунктуация цитаты DuckDB — две живые версии страницы расходятся в одной запятой,
  исходный markdown на GitHub отдаёт 404.
- Абсолютные числа из mssqltips 3323 — только в картинках, машинно не читаются.
- Байтовые размеры DECFLOAT в документации IBM — страницы `ibm.com/docs` рендерятся
  скриптом, отдают только навигацию. Значения 8/16 байт выводятся из параметров формата.
- Том/выпуск/страницы статьи в IBM JRD — Xplore и ACM DL недоступны, подтверждены только
  авторы, журнал и январь 2009.
- Ни одного полезного источника со Stack Overflow / dba.stackexchange найти не удалось;
  доменный поиск по этим сайтам возвращает ошибку прокси.
- Публичного бенчмарка TPC-H, который изолировал бы влияние DECIMAL-колонок в PostgreSQL,
  по-видимому, не существует.
- Не найдено письма, где Ян Вик своими словами обосновывает выбор varlena и BCD-упаковки, —
  это восстанавливается только по коду и по реплике Локхарта.

---

## 8. Что не поправлено, но стоит решить

1. **Объём.** Статья выросла до ~74 тыс. знаков. Естественный шов прежний: часть I —
   история, семь причин и патч; часть II — соседи, decimal128 и практика.
2. **Порядок публикации.** С учётом §1 письмо в hackers лучше отправить раньше статьи —
   иначе после ревью придётся править цифры и, возможно, вывод.
3. **Выравнивание.** `numeric_unpack_local` надо прогнать на строгой платформе
   (`--disable-spinlocks` тут не поможет — нужен реальный s390x или SPARC), потому что
   именно на этом посыпалась первая версия апрельского патча.
4. **КДПВ, хабы, теги** — по-прежнему не сделаны.
