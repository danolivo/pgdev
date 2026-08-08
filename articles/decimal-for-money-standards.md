# Точный десятичный тип для денег: что на самом деле требуют стандарты, право и практика

Исследование к статье «Почему тип numeric такой медленный». Вопрос простой: правда ли,
что `NUMERIC`/`DECIMAL` — стандарт (пусть де-факто) для финансовых приложений, или это
инженерный фольклор?

Короткий ответ: **формального требования «используйте NUMERIC для денег» не существует
нигде** — ни в ISO SQL, ни в законодательстве. Но существуют четыре независимых слоя
требований, которым двоичная плавающая точка не удовлетворяет по построению. При этом
половина того, что обычно приводят в подтверждение тезиса, при проверке первоисточников
не подтверждается.

Ниже — по пунктам, у каждого утверждения ссылка на источник. Всё, что не удалось
проверить, помечено явно.

**Оговорка о методе.** Все цитаты получены открытием страниц-первоисточников. Нормативный
текст ISO/IEC 9075 платный и недоступен — там, где это важно, сказано отдельно. Часть
российских ресурсов (`cbr.ru`, `base.garant.ru`, `glavbukh.ru`, `docs.cntd.ru`) не отдаётся
инструментам: либо JS-рендеринг, либо блокировка. Для них указано, что источник вторичный.

---

## 1. Чего в стандартах нет

### 1.1. В ISO SQL нет типа MONEY

Проверено по грамматике SQL:2016 Foundation
([извлечение Джейка Уита из ISO/IEC 9075-2:2016](https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html)):
токен `MONEY` в грамматике не встречается. То же самое по публичному тексту SQL-92
([ANSI draft, зеркало CMU](https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt)).

`money` в PostgreSQL и `money`/`smallmoney` в SQL Server — вендорские расширения, а не
реализация стандарта.

**Вывод:** стандарт даёт категорию «точный числовой тип», но нигде не говорит, чем
хранить деньги.

### 1.2. Что стандарт всё-таки говорит

SQL-92, подраздел 4.4 «Numbers»
([текст](https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt)):

> «The data types NUMERIC, DECIMAL, INTEGER, and SMALLINT are collectively referred to as
> exact numeric types.»

Разница между `NUMERIC` и `DECIMAL` — цитата стандарта, приведённая Томом Лейном в
pgsql-hackers ещё в 2000 году
([message-id `20835.948044134@sss.pgh.pa.us`](https://www.postgresql.org/message-id/20835.948044134%40sss.pgh.pa.us)),
и независимо у [datacadamia](https://datacadamia.com/sql/decimal):

> «NUMERIC specifies the data type exact numeric, with the decimal precision and scale
> specified by the \<precision\> and \<scale\>.»
>
> «DECIMAL specifies the data type exact numeric, with the decimal scale specified by the
> \<scale\> and the implementation-defined decimal precision **equal to or greater than**
> the value of the specified \<precision\>.»

То же у Гулуцана и Пельцера, [«SQL-99 Complete, Really», гл. 3](https://sql-99.readthedocs.io/en/latest/chapters/03.html).

**Практическое следствие:** `NUMERIC(15,2)` — контракт на ровно 15 цифр, `DECIMAL(15,2)` —
на «не меньше 15». В PostgreSQL это синонимы, но переносимость кода это затрагивает.

В SQL:2016 появилась и третья категория — `<decimal floating-point type>`, то есть
`DECFLOAT` (см. ту же грамматику).

### 1.3. Минимальной обязательной точности стандарт не задаёт

**НЕ ПОДТВЕРЖДЕНО.** Ни в одном доступном источнике не нашлось нормы, обязывающей
реализацию поддерживать какую-то минимальную точность; везде максимум объявлен
implementation-defined. MySQL формулирует только семантическое требование
([Fixed-Point Types](https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html)):

> «Standard SQL requires that `DECIMAL(5,2)` be able to store any value with five digits
> and two decimals.»

Утверждать про «стандарт требует минимум N цифр» не следует.

---

## 2. Международный уровень: где decimal фактически обязателен

### 2.1. Право

**ЕС, регламент Совета (EC) № 1103/97 о введении евро** — самый жёсткий из найденных
документов. [Полный текст на EUR-Lex](https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:31997R1103).

Статья 4:

> «The conversion rates shall be adopted with six significant figures.»
>
> «The conversion rates **shall not be rounded or truncated** when making conversions.»
>
> «Inverse rates derived from the conversion rates shall not be used.»
>
> «Monetary amounts to be converted from one national currency unit into another shall
> first be converted into a monetary amount expressed in the euro unit, which amount may
> be rounded to not less than three decimals… **No alternative method of calculation may
> be used unless it produces the same results.**»

Статья 5:

> «Monetary amounts to be paid or accounted for when a rounding takes place after a
> conversion into the euro unit pursuant to Article 4 shall be rounded up or down to the
> nearest cent. … **If the application of the conversion rate gives a result which is
> exactly half-way, the sum shall be rounded up.**»

Здесь три требования, каждое из которых двоичный float нарушает: шесть значащих
десятичных цифр без округления и усечения; запрет альтернативного метода, дающего иной
результат; и детерминированное поведение ровно на половине.

**Великобритания, НДС.** [HMRC VAT Trader Records, VATREC12030](https://www.gov.uk/hmrc-internal-manuals/vat-trader-records/vatrec12030):

> «If the VAT on any transaction comes to less than 0.5 of one penny, it should be rounded
> down. If the VAT comes to 0.5 of one penny or more, it should be rounded up.»

Обратите внимание на масштаб: правило формулируется в **долях пенни**, а
[VATREC12010](https://www.gov.uk/hmrc-internal-manuals/vat-trader-records/vatrec12010)
допускает «rounding down to one tenth of a penny» и «rounding to 3 digits». То есть
требуемая точность — не «два знака», а больше.

### 2.2. Форматы обмена финансовыми данными

**ISO 20022** (SWIFT, SEPA, платёжные системы). Из официальной XSD (`pacs.008.001.07`,
[копия схемы](https://raw.githubusercontent.com/yudhik/example-iso-20022/master/src/main/java/id/brainmaster/iso20022/model/pacs.008.001.07.xsd);
идентично в [`camt.053.001.05`](https://github.com/kedder/ofxstatement-iso20022/blob/master/doc/camt.053.001.05.xsd)):

```xml
<xs:simpleType name="ActiveCurrencyAndAmount_SimpleType">
    <xs:restriction base="xs:decimal">
        <xs:fractionDigits value="5"/>
        <xs:totalDigits value="18"/>
        <xs:minInclusive value="0"/>
    </xs:restriction>
</xs:simpleType>
<xs:complexType name="ActiveCurrencyAndAmount">
    <xs:simpleContent>
        <xs:extension base="ActiveCurrencyAndAmount_SimpleType">
            <xs:attribute name="Ccy" type="ActiveCurrencyCode" use="required"/>
        </xs:extension>
    </xs:simpleContent>
</xs:complexType>
```

То есть межбанковский стандарт — это структурно `NUMERIC(18,5)` плюс обязательный код
валюты. Прямого запрета на плавающую точку в тексте нет, но намерение однозначно: в
официальном JSON-биндинге суммы передаются **строками**
([ISO 20022 JSON Schema draft, 10.06.2025](https://www.iso20022.org/sites/default/files/media/file/ISO_20022_Generation_of_JSON_Schema_Draft_2020_12_for_ISO_20022_2013_10June2025.pdf)):

> «Number type are represented as strings because there was a preference for validating
> the total digits and fraction digits in the schema.»

Поверх этого действуют повалютные ограничения:
[xmldation, Currency decimals](https://knowledge.xmldation.com/support/iso20022/general_rules/currency_decimals) —
`<InstdAmt Ccy="EUR">10.403</InstdAmt>` отвергается с сообщением «Too many decimal digits
given. Maximum of 2 may be present for the given currency».

Отдельная деталь от представителя SWIFT
([Kris Ketels, W3C Web Payments WG](https://lists.w3.org/Archives/Public/public-payments-wg/2016May/0014.html)):
в ISO 20022 отрицательных сумм не бывает вовсе — дебет это или кредит, определяется
контекстом, а не знаком.

**XBRL** (отчётность для SEC/ESMA). Из схемы `xbrl-instance-2003-12-31.xsd`
([рендер Liquid Technologies](https://schemas.liquid-technologies.com/XBRL/2.1/xbrl-instance-2003-12-31_xsd.html)):

```xml
<simpleType name="monetary">
  <restriction base="decimal" />
</simpleType>
```

Нюанс, который стоит знать: атрибуты `@decimals` и `@precision` в XBRL — это **метаданные
о точности реального факта**, а не объявление точности хранения. Рабочая записка XBRL
[«Precision, Decimals and Units 1.0»](http://www.xbrl.org/WGN/precision-decimals-units/WGN-2017-01-11/precision-decimals-units-WGN-2017-01-11.html):

> «The `@precision` and `@decimals` attributes indicate the range in which the actual value
> of the fact that gave rise to its expressed value in the XBRL instance lies.»

**FIX** (биржевая торговля). В tag-value FIX 4.4 тип `float` определён как символьная
строка ([FIX 4.4 dictionary, Onix](https://www.onixs.biz/fix-dictionary/4.4/index.html)):

> «Sequence of digits with optional decimal point and sign character… All float fields must
> accommodate up to fifteen significant digits.»

В FIXML привязка явная
([FIX Latest, FIXML datatypes](https://fiximate.fixtrading.org/en/FIX.Latest/fixml_datatypes.html)):
`float`, `Qty`, `Price`, `Amt`, `Percentage` → `xs:decimal`.

А в бинарной кодировке SBE сказано прямо
([FIX SBE v1.0 RC4, Field Encoding](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding/blob/master/v1-0-RC4/doc/02FieldEncoding.md)):

> «Decimal encodings should be used for prices and related monetary data types like
> PriceOffset and Amt.»
>
> «Binary floating point encodings are compatible with IEEE Standard for Floating-Point
> Arithmetic (IEEE 754-2008). They should be used for floating point numeric fields **that
> do not represent prices or monetary amounts**.»

> ⚠️ **Осторожно с распространённой цитатой.** Часто пишут, что спецификация FIX «прямо
> оговаривает: наш float — не IEEE». В таблице типов tag-value FIX 4.4 такой фразы найти
> не удалось. Суть верна, но приписывать этот текст tag-value спецификации не надо.

### 2.3. Валюты: ISO 4217 делает масштаб свойством валюты

[Страница ISO об ISO 4217](https://www.iso.org/iso-4217-currency-codes.html):

> «For currencies having minor units, ISO 4217:2015 also shows the relationship between the
> minor unit and the currency itself (i.e. whether it divides into 100 or 1000).»
>
> «…managed by the Secretariat of the Maintenance Agency, in this case the SIX Financial
> Information AG…»

Авторитетный список — [SIX Group, list-one.xml](https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml).
Сам файл инструментами не отрендерился; значения ниже взяты из
[воспроизведения того же файла в проекте piecash](https://piecash.readthedocs.io/en/master/_modules/piecash/core/currency_ISO.html),
поле `CcyMnrUnts`:

| Валюта | Знаков |
|---|---|
| JPY, CLP, ISK, XOF | **0** |
| USD, EUR | 2 |
| KWD, BHD, OMR, JOD, TND | **3** |

Отсюда прямое инженерное следствие: «два знака после запятой» — не универсальная
константа, а параметр модели данных.

### 2.4. Отраслевые бенчмарки: показательный раскол

**TPC-C**, [Standard Specification Rev. 5.11, клауза 1.3.1](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf) —
самая сильная формулировка из всех найденных:

> «Numeric fields that contain monetary values (W_YTD, D_YTD, C_CREDIT_LIM, C_BALANCE,
> C_YTD_PAYMENT, H_AMOUNT, OL_AMOUNT, I_PRICE) **must use data types that are defined by
> the DBMS as being an exact numeric data type** or that satisfy the ANSI SQL Standard
> definition of being an exact numeric representation.»

Колонки объявлены как `signed numeric(12,2)`, `signed numeric(6,2)` и т. п.

**TPC-E** (брокерский бенчмарк),
[v1.14.0, клауза 2.2.1](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-e_v1.14.0.pdf):

> «ENUM and SENUM… must be implemented using a Native Data Type which provides **exact
> representation** of at least n Digits of precision after the decimal place.»
>
> «BALANCE_T is defined as SENUM(12,2)… FIN_AGG_T is defined as SENUM(15,2)…»

**А вот TPC-H и TPC-DS — нет.**
[TPC-H v3.0.1, клауза 1.3.1](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf):

> «Decimal means that the column must be able to represent values in the range
> −9,999,999,999.99 to +9,999,999,999.99 in increments of 0.01; the values can be **either
> represented exactly or interpreted to be in this range**»

И там же допуск при валидации (клауза 2.1.3.5): «within $100» для `SUM` и «within 1 %» для
`AVG`. [TPC-DS v3.2.0, клауза 2.2.2.1](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf)
содержит ту же лазейку — причём для `Integer` требование точности сформулировано жёстко,
а для `Decimal` нет.

**Вывод:** транзакционные бенчмарки требуют точного типа дословно, аналитические —
явно разрешают приближение. Этот раскол сам по себе — хороший ответ на вопрос «где
numeric обязателен, а где нет».

### 2.5. Что говорят вендоры СУБД

**PostgreSQL**, [Numeric Types](https://www.postgresql.org/docs/current/datatype-numeric.html):

> «The type `numeric` can store numbers with a very large number of digits. **It is
> especially recommended for storing monetary amounts** and other quantities where
> exactness is required. … **However, calculations on `numeric` values are very slow
> compared to the integer types, or to the floating-point types**…»
>
> (в разделе 8.1.3) «If you require exact storage and calculations (such as for monetary
> amounts), use the `numeric` type instead.»

И на [странице типа `money`](https://www.postgresql.org/docs/current/datatype-money.html):

> «Floating point numbers should not be used to handle money due to the potential for
> rounding errors.»

**Microsoft SQL Server** — самая категоричная формулировка,
[float and real](https://learn.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql):

> «Because of the approximate nature of the float and real data types, **don't use these
> data types when exact numeric behavior is required. Examples that require precise numeric
> values are financial or business data**, operations involving rounding, or equality
> checks. In those cases, use the integer, decimal, numeric, money, or smallmoney data
> types.»

**MySQL**, [Fixed-Point Types](https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html):

> «The DECIMAL and NUMERIC types store exact numeric data values. These types are used when
> it is important to preserve exact precision, **for example with monetary data**.»

**Oracle** — только механизм, про деньги не пишет.
[SQL Language Reference 23](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html):

> «Values are stored using decimal precision for `NUMBER`. … Binary floating-point numbers
> are stored using binary precision… Such a storage scheme cannot represent all values
> using decimal precision exactly.»

[Database Development Guide](https://docs.oracle.com/en/database/oracle/oracle-database/18/adfns/sql-data-types.html):

> «NUMBER values are stored in decimal format. **For calculations that need decimal
> rounding, use the NUMBER data type.**»

**IBM** — прямого запрета в документации Db2 найти не удалось. Зато есть материал
Майка Каулишо (написан в IBM),
[Decimal Arithmetic FAQ](https://speleotrove.com/decimal/decifaq1.html), с самым известным
расчётом в этой теме:

> «Consider the calculation of a 5% sales tax on an item (such as a $0.70 telephone call),
> which is then rounded to the nearest cent. Using double binary floating-point, the result
> of 0.70 × 1.05 is 0.73499999999999998667732370449812151491641998291015625; the result
> should have been 0.735 (which would be rounded up to $0.74) but instead the rounded result
> would be $0.73.»
>
> «**Taken over a million transactions of this kind… these systematic errors add up to an
> overcharge of more than $20. For a large company, the million calls might be
> two-minutes-worth; over a whole year the error then exceeds $5 million.**»

Там же — данные о том, как реально типизированы коммерческие базы:

> «Of the numeric columns, the breakdown by datatype was: Decimal 251,038 columns (55.0%),
> SmallInt 120,464 (26.4%), Integer 78,842 (17.3%), Float 6,180 (1.4%).»
>
> «These figures indicate that almost all (98.6%) of the numbers in commercial databases
> have a decimal or integer representation.»

---

## 3. Российская Федерация

Здесь картина устроена иначе, чем на Западе: технических стандартов на представление
денег нет вовсе, зато налоговое и бухгалтерское право описывает **поведение**, которому
`numeric` удовлетворяет, а `double precision` — нет.

### 3.1. Налоговый кодекс: два масштаба одной суммы

**Пункт 6 статьи 52 НК РФ**
([КонсультантПлюс](https://www.consultant.ru/document/cons_doc_LAW_19671/bbc7b0201b7be7a79e0b44464f1f2fa071d9a774/),
[rulaws.ru](https://rulaws.ru/nk-rf-chast-1/Razdel-IV/Glava-8/Statya-52/)):

> «Сумма налога, сбора исчисляется в полных рублях. Сумма налога менее 50 копеек
> отбрасывается, а сумма налога 50 копеек и более округляется до полного рубля. Сумма
> сбора округляется до полного рубля.»

Норма введена Федеральным законом от 23.07.2013 № 248-ФЗ (действует с 2014 года), третье
предложение добавлено Федеральным законом от 31.07.2023 № 389-ФЗ. Устаревшую редакцию без
слова «сбора» цитировать не следует.

**А в счёте-фактуре округление запрещено.** Постановление Правительства РФ от 26.12.2011
№ 1137, приложение № 1, Правила заполнения счёта-фактуры, пункт 3
([Контур.Норматив](https://normativ.kontur.ru/document?documentId=504130&moduleId=1)):

> «Стоимостные показатели счета-фактуры (в графах 4 - 6, 8 и 9) указываются **в рублях и
> копейках** (долларах США и центах, евро и евроцентах либо в другой валюте).»

Разъяснения ведомств прямо разводят два правила. Письмо Минфина России от 01.04.2014
№ 03-07-РЗ/14417 ([na.buhgalteria.ru](http://na.buhgalteria.ru/document/n129236)):

> правило об округлении, предусмотренное пунктом 6 статьи 52 Кодекса, «в отношении сумм
> налога на добавленную стоимость, предъявляемых продавцами покупателям товаров (работ,
> услуг) и указываемых в счетах-фактурах и в книге продаж, **не применяется**»

Письмо ФНС России от 07.04.2014 № ГД-4-3/6398@ и письмо Минфина от 15.10.2019
№ 02-07-10/79001 — то же самое
([подборка КонсультантПлюс](https://www.consultant.ru/law/podborki/okruglenie_kopeek_v_schetah-fakturah/)).

**Это ключевая для инженера конструкция.** Одна и та же сумма живёт в системе в двух
представлениях, а переход между ними — не форматирование, а нормативно предписанная
операция, применяемая ровно один раз, в строго определённой точке, и запрещённая во всех
остальных. Тип данных, который округляет сам, при каждом сложении, соблюсти это правило
не позволяет в принципе.

| Уровень | Точность | Норма |
|---|---|---|
| Сумма налога/сбора к уплате | полные рубли, half-up от 50 коп. | п. 6 ст. 52 НК РФ |
| Стоимостные показатели и НДС в счёте-фактуре | рубли и копейки | п. 3 Правил (ПП № 1137) + письма Минфина/ФНС |

### 3.2. Бухгалтерский учёт

**Федеральный закон № 402-ФЗ, статья 12** «Денежное измерение объектов бухгалтерского
учёта» ([zakonrf.info](https://www.zakonrf.info/zakon-o-buhuchete/12/)):

> «1. Объекты бухгалтерского учета подлежат денежному измерению.
> 2. Денежное измерение объектов бухгалтерского учета производится в валюте Российской
> Федерации.»

Требований к точности в 402-ФЗ **нет** — только валюта. Точность спускается на уровень
отраслевых актов.

**ФСБУ 4/2023 «Бухгалтерская (финансовая) отчётность»** (применяется с отчётности за
2025 год), пункт 60, подпункт «г»
([Контур.Норматив](https://normativ.kontur.ru/document/1/503147-prikaz-minfina-rf-ot-04-10-2023-n-157n),
[КонсультантПлюс](https://www.consultant.ru/document/cons_doc_LAW_472684/3bbdfc84c3f1e0767475fa44da787005cfc86fb7/)):

> «г) формат представления значений показателей. В бухгалтерской отчетности значения
> показателей представляются **в тысячах рублей**, если иное не установлено настоящим
> Стандартом или другими федеральными стандартами.»

Отдельного пункта про правила округления в стандарте нет: тысячи рублей — масштаб
**отчётной формы**, а не учётных данных. Приказ Минфина № 66н со старыми формами утратил
силу с 1 января 2025 года (п. 3 приказа Минфина от 04.10.2023 № 157н, там же).

### 3.3. Банк России

**Учёт в рублях и копейках.** Дословную норму в действующем Положении № 809-П
**открыть не удалось** (сайты не отдают текст инструментам). Подтверждается по
предшественнику — Положению Банка России от 27.02.2017 № 579-П, Часть I «Общая часть»
([КонсультантПлюс](http://www.consultant.ru/document/cons_doc_LAW_213488/2a83f5d935169a7aa6005d5fcf1af424a43a64c7/)):

> п. 5: «Бухгалтерский учет совершаемых операций… ведется в валюте Российской Федерации.»
>
> п. 28: «Первичные балансы… составляются **в рублях и копейках**.»

Переносить это на 809-П без сверки первоисточника не следует.

**Положение № 762-П, реквизиты платёжного поручения** — источник вторичный
([PDF приложения 1 на klerk.ru](https://www.klerk.ru/ugc/attachments/pdf/73daf6edf28d69dd8228.pdf),
[расшифровка полей](https://veq.ru/catalog/buh/doc/1871)):

> Реквизит 6 «Сумма прописью»: «…слово "рубль" в соответствующем падеже не сокращается,
> **копейки указываются цифрами**»
>
> Реквизит 7 «Сумма»: «сумма платежа цифрами, рубли отделяются от копеек знаком тире "-"»

Любопытная деталь: сумма передаётся в двух независимых представлениях — прописью и
цифрами. Классическая контрольная сумма на уровне документа, унаследованная из бумажного
оборота.

> ⚠️ Положение 762-П существенно изменено Указанием от 17.06.2025; с 2026 года вводится
> форма «платёжное распоряжение» 0401069. Нумерация реквизитов могла измениться — перед
> публикацией сверяться с cbr.ru.

**Миграция на ISO 20022 — ПОДТВЕРЖДЕНО.**
[Раздел «Стандарт ISO 20022» на сайте ЦБ](https://www.cbr.ru/psystem/iso_20022/):

> «одной из приоритетных задач Банка России является внедрение международного стандарта
> ISO 20022»

«Единый план миграции… в национальной платежной системе» (утв. Банком России 12.03.2024
№ ПМ-04-45-1-3/45, [файл ЦБ](https://www.cbr.ru/Crosscut/LawActs/File/7708),
[КонсультантПлюс](https://www.consultant.ru/document/cons_doc_LAW_472986/)) охватывает
СПФС, платёжную систему Банка России, системы Федерального казначейства и ЦИК. Ключевые
вехи по таблице этапов: II кв. 2025 — обязательность стандартов при платёжных
распоряжениях; III кв. 2028 — переход с УФЭБС с конвертацией; **III кв. 2029 — полный
переход ПС БР на альбом ISO 20022**.

Практический смысл для проектирования: к концу десятилетия российская платёжная
инфраструктура окажется на формате, где сумма — `xs:decimal` с явным числом знаков и
обязательным кодом валюты.

### 3.4. ОКВ не содержит разрядности валют

Действует **ОК (МК (ИСО 4217) 003-97) 014-2000**, утверждён Постановлением Госстандарта
от 25.12.2000 № 405-ст; ОК 014-94 [прекратил действие](https://base.garant.ru/570149/).

Из введения ([Контур.Норматив](https://normativ.kontur.ru/document?documentId=504497&moduleId=1)):

> «ОКВ гармонизирован с Международным стандартом ИСО 4217-2000 "Коды для представления
> валют и фондов"»

Но структура позиции — только коды, наименования валют и признаки стран. **Поля
разменных единиц (minor units) в ОКВ нет**, в отличие от таблицы A.1 самого ISO 4217.

**Следствие:** машиночитаемую разрядность дробной части произвольной валюты из российских
нормативных актов получить нельзя — приходится брать из ISO 4217 напрямую или из
справочников платёжных систем.

### 3.5. ГОСТов на представление денежных величин нет

- **ГОСТ Р ИСО 20022-1-2013** «Финансовые услуги. Универсальная схема сообщений финансовой
  индустрии. Часть 1. Метамодель» — существует, введён с 1 октября 2014 года
  ([docs.cntd.ru](https://docs.cntd.ru/document/1200108898),
  [standartgost.ru](https://standartgost.ru/g/%D0%93%D0%9E%D0%A1%D0%A2_%D0%A0_%D0%98%D0%A1%D0%9E_20022-1-2013)).
  Принята **только часть 1**; остальных частей как ГОСТ Р в открытых каталогах нет.
- **ГОСТ Р ИСО 4217 не существует.** В каталоге ФГБУ «Институт стандартизации» ISO 4217
  значится как международный стандарт, не адаптированный в качестве национального
  ([карточка ISO 4217:2008](https://www.gostinfo.ru/catalog/Details/?id=4190620)).
  Российский эквивалент — не ГОСТ, а классификатор ОКВ.
- ГОСТ Р 57580.1-2017 / 57580.2-2018 существуют, но касаются защиты информации, а не
  представления чисел.
- **Аналога IEEE 754 decimal или ISO/IEC 10967 в виде ГОСТ Р в финансовой области нет.**

### 3.6. ФФД (54-ФЗ): деньги — целое число копеек

Самая интересная находка по РФ. Приказ ФНС от 14.09.2020 № ЕД-7-20/662@, приложение № 2
(и идентично в предшествующем ММВ-7-20/229@).

Определения типов, раздел I
([sudact.ru](https://sudact.ru/law/prikaz-fns-rossii-ot-14092020-n-ed-7-20662/prilozhenie-n-2/i_1/),
[КонсультантПлюс](https://www.consultant.ru/document/cons_doc_LAW_214339/0ca1f4a1abc08962fede832bd65a2c3f5b0141ca/)):

> **VLN** — «целое число без знака, представленное в электронной форме в виде
> последовательности из нескольких байтов, варьируемой длины»
>
> **FVLN** — «число с точкой без знака… первый байт определяет положение десятичной точки»

Таблица 5 «Описание общих значений реквизитов ФД»
([sudact.ru](https://sudact.ru/law/prikaz-fns-rossii-ot-14092020-n-ed-7-20662/prilozhenie-n-2/i_1/tablitsa-5/),
[КонсультантПлюс](https://www.consultant.ru/document/cons_doc_LAW_214339/f3b87e8f98ff78a4c457fe46ca49d700eb364edc/)):

| Реквизит | Тег | Тип | Байт | Описание |
|---|---|---|---|---|
| сумма расчёта, указанного в чеке | 1020 | **VLN** | 6 | «Величина с учетом копеек, печатается в виде числа с фиксированной точкой (2 цифры после точки) в рублях» |
| стоимость предмета расчёта с учётом скидок | 1043 | VLN | 6 | то же |
| цена за единицу предмета расчёта | 1079 | VLN | 6 | то же |
| сумма по чеку наличными | 1031 | VLN | 6 | то же |
| сумма НДС за предмет расчёта | 1200 | VLN | 6 | то же |
| **количество предмета расчёта** | 1023 | **FVLN** | 8 | «Если количество измеряется целым числом, точка… может не использоваться» |

Три вывода:

1. Деньги — беззнаковое целое на 6 байт, до 2⁴⁸−1 ≈ 2,8·10¹⁴ копеек ≈ 2,8 трлн рублей.
   Ни плавающей точки, ни строк.
2. Количество — единственное поле с дробной частью, и под него введён **отдельный тип**.
   Авторы формата сознательно развели «деньги = целое» и «количество = дробное».
3. Разделение «хранение в копейках / печать в рублях с двумя знаками» прописано прямо в
   нормативном акте.

Это прямой российский аналог подхода Stripe — и, что важнее, он введён **обязательным**
для всей розницы.

### 3.7. Форматы ФНС для ЭДО: наоборот, decimal

Нотация ФНС из приложения к формату счёта-фактуры/УПД
([base.garant.ru/408449233](https://base.garant.ru/408449233/f7ee959fd36b5699076b35abf4f52c5c/)):

> «Т — текстовое поле; N — числовое поле (число или дробное число)»; `N(m.k)`, где m —
> общая разрядность, k — разрядность дробной части.

В XSD (`ON_SCHFDOPPR_1_995_01_05_01_05.xsd` на nalog.gov.ru) стоимостные показатели
объявлены как `xs:decimal` с ограничениями:

```xml
<xs:attribute name="СтТовБезНДС">
  <xs:simpleType>
    <xs:restriction base="xs:decimal">
      <xs:totalDigits value="26"/>
      <xs:fractionDigits value="11"/>
    </xs:restriction>
  </xs:simpleType>
</xs:attribute>

<xs:attribute name="СтТовБезНДСВсего">
  <xs:simpleType>
    <xs:restriction base="xs:decimal">
      <xs:totalDigits value="19"/>
      <xs:fractionDigits value="2"/>
    </xs:restriction>
  </xs:simpleType>
</xs:attribute>
```

Кросс-проверка по табличной части приказа
([Таблица 5.12 «Сведения о товаре», КонсультантПлюс](https://www.consultant.ru/document/cons_doc_LAW_367925/aac3381537a36ba09b3b69f2ebbedbf84f8df6e2/)):
`ЦенаТовДо`/`ЦенаТовПосле` объявлены как `N(26.11)` — совпадает с `totalDigits=26 /
fractionDigits=11`.

> ⚠️ ЧАСТИЧНО. Схема читалась через инструмент, пропускающий страницу через модель;
> точное распределение конкретных атрибутов по масштабам перед публикацией стоит сверить
> по скачанной XSD. Безопасное утверждение: **ФНС использует `xs:decimal` с
> `totalDigits`/`fractionDigits`, а не строки с шаблоном; в строках табличной части
> 11 знаков дробной части, в итогах — 2.**

Получается любопытная симметрия: **одно ведомство в двух своих форматах решает задачу
противоположными способами** — целые копейки в ФФД и `decimal(26,11)` в ЭДО, — но оба раза
сознательно уходит от плавающей точки.

### 3.8. УФЭБС Банка России — НЕ ПРОВЕРЕНО по первоисточнику

Альбом форматов лежит на [cbr.ru/development/formats](https://www.cbr.ru/development/formats/)
(например, `UFEBS_v2026_09_0.zip`, `UFEBS_XSDDoc.zip`), но скачать и распаковать архивы не
удалось — сайт не отдаётся инструментам, а ZIP они не разбирают. Дословную цитату из схемы
привести не могу.

Косвенные признаки, что атрибут `Sum` в ED101 — целое число копеек без разделителя:
[пример построения ED101 у интегратора](https://isupport.softlab.ru/portal/Samples/sample.asp?id=246)
(`addAttr(ed101, "Sum", 2400000)`), библиотека
[creepycheese/ruby_ufebs](https://github.com/creepycheese/ruby_ufebs) (`sum: 150000` →
`Sum="150000"`). Для публикации надо скачать альбом вручную.

### 3.9. СБП: в QR-ссылке — копейки

Официальной спецификации НСПК в открытом доступе нет. По документации интеграторов
картина воспроизводится дважды независимо:

- [Монета, протокол C2B](https://docs.moneta.ru/sbp/c2b-reusable-qr/index.html): в запросе
  `<mes:amount>10.00</mes:amount>`, в payload QR — `sum=1000`, ссылка вида
  `https://qr.nspk.ru/AS100074QCPTCVTO8JM9NO8IDAIQPHPB?type=01&bank=...&sum=1000&cur=RUB&crc=2FF3`
- [PayAnyWay, InvoiceRequest](https://docs.payanyway.ru/marketplace/sbp/protokol-c2b.-vystavlenie-schyota-invoicerequest-dlya-oplaty-po-dinamicheskomu-qr):
  в запросе `amount = "100"`, в `qrpayload` — `sum=10000`

Соотношение 10.00 → 1000 и 100 → 10000 воспроизводится, но **прямой формулировки «в
копейках» в спецификации найти не удалось** — это вывод из примеров.

### 3.10. 1С

**Тип «Число» в платформе** —
[ИТС, «Разрядность результатов выражений и агрегатных функций»](https://its.1c.ru/db/content/metod8dev/src/developers/platform/metod/query/i8102665.htm):
максимальная разрядность 38 знаков, нотация вида `Число(17,4)`, правила вывода разрядности
результата для сложения, умножения и деления. Там же оговорка: **«В DB2 максимум составляет
31, а не 38 знаков»** — косвенное, но сильное свидетельство, что «Число(N,M)» ложится прямо
в `DECIMAL/NUMERIC(N,M)` СУБД, раз ограничение платформы наследуется от ограничения СУБД.

**Официального маппинга «Число(N,M) → numeric(N,M)» найти не удалось.** Ни
[ИТС «Размещение данных 1С:Предприятия 8»](https://its.1c.ru/db/content/metod8dev/src/admins/i8101798.htm),
ни [статья 1С на Хабре про работу с разными СУБД](https://habr.com/ru/companies/1c/articles/753242/)
таблицы соответствия типов не содержат. Утверждать без первоисточника не следует.

**Зато есть официальный формат 1С с деньгами.** Стандарт DirectBank,
[1C-Company/DirectBank](https://github.com/1C-Company/DirectBank), файл
[`doc/xsd-scheme/1C-Bank_Exch-Common.xsd`](https://github.com/1C-Company/DirectBank/blob/master/doc/xsd-scheme/1C-Bank_Exch-Common.xsd):

```xml
<xsd:simpleType name="SumType">
  <xsd:annotation>
    <xsd:documentation>Сумма в документе</xsd:documentation>
  </xsd:annotation>
  <xsd:restriction base="xsd:decimal">
    <xsd:totalDigits value="18"/>
    <xsd:fractionDigits value="2"/>
  </xsd:restriction>
</xsd:simpleType>
```

`decimal(18,2)` — ровно то, что в PostgreSQL было бы `numeric(18,2)`.
([Описание схем на сайте 1С](https://v8.1c.ru/tekhnologii/obmen-dannymi-i-integratsiya/standarty-i-formaty/standart-vzaimodeystviya-po-tekhnologii-directbank/opisanie-standarta-vzaimodeystviya-mezhdu-1s-predpriyatie-8-i-bankovskim-servisom/skhemy-dannykh/))

### 3.11. Российские платёжные API: тот же раскол, что и в мире

**T-Bank / Тинькофф Касса — целые копейки.**
[Метод `Init`, developer.tbank.ru](https://developer.tbank.ru/eacq/api/init):

> `Amount` — `Integer<int64>`. «**Сумма в копейках. Например, 3 руб. 12коп. — это число
> 312.**»

Причём поля внутри `Items` прямо промаркированы тегами ФФД (`Amount` — тег 1043, `Price` —
тег 1078). То есть целые копейки здесь наследуются из фискального формата.

**ЮKassa — строка с двумя знаками.**
[Официальный пример запроса](https://yookassa.ru/developers/payment-acceptance/integration-scenarios/manual-integration/bank-card):

```json
{ "amount": { "value": "2.00", "currency": "RUB" }, ... }
```

То же в [«Формате ответа»](https://yookassa.ru/developers/using-api/response-handling/response-format):
`"amount": { "value": "100.00", "currency": "RUB" }`.

**Robokassa — десятичное через точку**, и с интересным следствием.
[docs.robokassa.ru, «Интерфейс оплаты»](https://docs.robokassa.ru/ru/pay-interface):

> `OutSum` — «Сумма к оплате. Формат — число через точку, например `123.45`.»

`OutSum` в исходном текстовом виде входит в строку подписи
`MerchantLogin:OutSum:InvId:...:Пароль#1:Shp_*`. То есть **строковое представление суммы
криптографически значимо**: `123.4` и `123.40` дадут разные подписи. Ещё один аргумент
против хранения денег в типе, который «сам решает», как их печатать.

**Сбербанк Эквайринг — минимальные единицы валюты.** Официальная вики не открылась;
формулировка приводится в [разборе на Хабре](https://habr.com/ru/articles/723078/):
«Сумма платежа в минимальных единицах валюты (копейки, центы и т.д.)». Перед публикацией
сверить по первоисточнику.

**CloudPayments — дробное в рублях.** Из описания платёжного виджета на
[developers.cloudpayments.ru](https://developers.cloudpayments.ru/): `amount` — `float`,
«Сумма платежа. Должно быть > 0. Пример: 2; 2.5; 2.57». Справочник API — SPA, таблицу
параметров вытащить не удалось.

### 3.12. Правоприменение: округление меняет правовой результат

**Решение ВАС РФ от 20.08.2012 № 8116/12.** Суд признал недействующим пункт 2.11 Порядка
заполнения декларации по УСН, предписывавший округление стоимостных показателей до полных
рублей ([разбор с цитатами](https://www.nalog-briz.ru/2012/12/20082012-811612.html);
[подборка КонсультантПлюс](https://www.consultant.ru/law/podborki/okrugleniya_v_deklaracii_po_usn/);
[ГАРАНТ](https://www.garant.ru/company/garant-press/ab/434294/4/)):

> «При сопоставлении сумм налога, исчисленных исходя из округленных стоимостных
> показателей и без их округления, суд приходит к выводу, что установленное пунктом 2.11
> Порядка правило округления **приводит к изменению налоговой обязанности
> налогоплательщика**.»

> «Порядок исчисления налога с применением округления стоимостных показателей установлен
> Кодексом непосредственно для определения налоговой базы по налогу на доходы физических
> лиц… и единого налога на вмененный доход, для упрощенной системы налогообложения такого
> порядка исчисления налога Кодексом не предусмотрено.»

Ход рассуждения суда буквально совпадает с инженерным: округление промежуточных значений
меняет результат; правило округления действует только там, где прямо предписано, и не
распространяется по аналогии.

### 3.13. Итог по РФ

Прямого предписания «используйте точный десятичный тип» в российском законодательстве
**нет**. Но из совокупности норм следует функционально эквивалентное требование:

1. **Хранение обязано быть точным до копейки** — ПП № 1137 требует рубли и копейки в
   счёте-фактуре, письма Минфина и ФНС запрещают там округление. Двоичный float не
   представляет 0,01 точно, значит система на `double precision` нарушает это по
   построению.
2. **Округление — нормируемая операция в единственной точке**, а не побочный эффект
   арифметики.
3. **Незаконное округление меняет налоговую обязанность** — это установлено судом, а не
   только логикой.
4. **Разрядность зависит от валюты, и российские классификаторы её не дают** — значит
   масштаб должен быть параметром модели, а не константой.

Закон не называет тип, но описывает поведение, которому в PostgreSQL удовлетворяет ровно
`numeric`: точное десятичное хранение, отсутствие неявного округления, явно управляемый
масштаб.

---

## 4. Языки и фреймворки: где кончается документация и начинается фольклор

### 4.1. Что действительно написано в документации

**.NET** — самая прямая формулировка из всех,
[System.Decimal](https://learn.microsoft.com/en-us/dotnet/api/system.decimal):

> «The Decimal value type is **appropriate for financial calculations** that require large
> numbers of significant integral and fractional digits and no round-off errors.»
>
> «The Decimal type does not eliminate the need for rounding. Rather, it minimizes errors
> due to rounding.»

**Python** — лучше всех аргументировано,
[модуль decimal](https://docs.python.org/3/library/decimal.html):

> «In decimal floating point, `0.1 + 0.1 + 0.1 - 0.3` is exactly equal to zero. In binary
> floating point, the result is `5.5511151231257827e-017`. … **For this reason, decimal is
> preferred in accounting applications which have strict equality invariants.**»
>
> «The decimal module incorporates a notion of significant places so that `1.30 + 1.20` is
> `2.50`. The trailing zero is kept to indicate significance. **This is the customary
> presentation for monetary applications.**»

Второй аргумент — про значимость хвостового нуля — это ровно `dscale` из PostgreSQL.

### 4.2. Чего в документации нет, вопреки общему убеждению

- **Javadoc `java.math.BigDecimal` про деньги не говорит ни слова.**
  [Описание класса](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/math/BigDecimal.html) —
  «Immutable, arbitrary-precision signed decimal numbers» — чисто математическое, слова
  money, currency, financial в нём отсутствуют.
- **Django не говорит «используйте DecimalField для денег».**
  [Model field reference](https://docs.djangoproject.com/en/5.2/ref/models/fields/) объясняет
  механическую разницу `FloatField` vs `DecimalField` и отсылает к документации Python.
- **Rails тоже не говорит.**
  [Active Record Migrations](https://guides.rubyonrails.org/active_record_migrations.html)
  показывает `add_column :products, :price, :decimal, precision: 5, scale: 2` — и всё.
  Зато главная Ruby-библиотека для денег делает наоборот:
  [RubyMoney/money](https://github.com/RubyMoney/money) — «Represents monetary values as
  integers, in cents. This avoids floating point rounding errors.»
- **SQLAlchemy** ([sqltypes.py](https://raw.githubusercontent.com/sqlalchemy/sqlalchemy/main/lib/sqlalchemy/sql/sqltypes.py))
  предупреждает только про потерю точности на стыке с DBAPI, про деньги не пишет.

### 4.3. Два самых цитируемых авторитета говорят не то, что им приписывают

**Джошуа Блох, Effective Java, Item 60** — «Avoid float and double if exact answers are
required» ([английский текст](https://github.com/clxering/Effective-Java-3rd-edition-Chinese-English-bilingual/blob/dev/Chapter-9/Chapter-9-Item-60-Avoid-float-and-double-if-exact-answers-are-required.md)):

> «In summary, don't use float or double for any calculations that require an exact answer.
> Use BigDecimal if you want the system to keep track of the decimal point and you don't
> mind the inconvenience and cost of not using a primitive type… **If performance is of the
> essence… use int or long.** If the quantities don't exceed nine decimal digits, you can
> use int; if they don't exceed eighteen digits, you can use long.»

Запрет у него один — на двоичный float. `BigDecimal` и `int`/`long` предлагаются как
равноправные варианты.

**Мартин Фаулер, PoEAA, гл. 18, паттерн Money.**
[Страница каталога](https://martinfowler.com/eaaCatalog/money.html) содержит только
постановку задачи. Сама рекомендация — в тексте книги:

> «You can store the amount as either an integral type or a fixed decimal type. The decimal
> type is easier for some manipulations, the integral for others. **You should absolutely
> avoid any kind of floating point type**, as that will introduce the kind of rounding
> problems that Money is intended to avoid.»

> ⚠️ Цитата воспроизведена по [публичной копии текста книги](https://gist.github.com/cryptocompress/7097498);
> на martinfowler.com её нет. При публикации ссылаться на книгу (PoEAA, гл. 18 «Money»), а
> не на URL.

Фаулер тоже агностичен между целым и десятичным. Его нерушимое требование — **валюта в
типе**, а не десятичность представления.

**JSR 354 (Java Money) намеренно отказывается фиксировать представление.**
[`javax.money.MonetaryAmount`](https://javamoney.github.io/apidocs/javax/money/MonetaryAmount.html):

> «JSR 354 explicitly supports different types of monetary amounts to be implemented and
> used. Reason behind is that the requirements to an implementation heavily vary for
> different usage scenarios.»

И референсная реализация везёт обе:
[`Money`](https://raw.githubusercontent.com/JavaMoney/jsr354-ri/master/moneta-core/src/main/java/org/javamoney/moneta/Money.java)
на `BigDecimal` и
[`FastMoney`](https://raw.githubusercontent.com/JavaMoney/jsr354-ri/master/moneta-core/src/main/java/org/javamoney/moneta/FastMoney.java)
на `long` в minor units, про который в javadoc написано:

> «It suggested to have a performance advantage of a 10-15 times faster compared to
> `Money`, which internally uses `BigDecimal`. Nevertheless this comes with a price of less
> precision.»

### 4.4. Отдельный миф: «в финансах обязательно банковское округление»

**Это неверно, причём ровно наоборот.**

Майк Каулишо, [Decimal Arithmetic Specification](https://speleotrove.com/decimal/damodel.html):

> «*round-half-even* is often used for other applications in the USA, where it is usually
> called "round to nearest" and is sometimes called "banker's rounding".»
>
> «*round-half-up* is the usual round-to-nearest algorithm used in European countries, **in
> international financial dealings, and in the USA for tax calculations**.»

IEEE 754-2008, клаузы 4.3.1 и 4.3.3
([текст стандарта](https://www.dsc.ufcg.edu.br/~cnum/modulos/Modulo2/IEEE754_2008.pdf)):

> «The roundTiesToEven rounding-direction attribute shall be the **default** rounding-direction
> attribute for results **in binary formats**.»
>
> «**A decimal format implementation of this standard shall provide roundTiesToAway as a
> user-selectable rounding-direction attribute.** The rounding attribute roundTiesToAway is
> not required for a binary format implementation.»

То есть half-even — это дефолт IEEE 754 для **двоичной** арифметики, а стандарт специально
обязывает десятичные реализации поддерживать коммерческое half-away-from-zero. И
законодательство это подтверждает: ЕС (регламент 1103/97, ст. 5) и HMRC требуют half-**up**;
п. 6 ст. 52 НК РФ — тоже half-up («50 копеек и более округляется до полного рубля»).

---

## 5. Где практика расходится со стандартами: целые в minor units

### 5.1. Платёжные API

| Провайдер | Представление | Тип в JSON |
|---|---|---|
| [Stripe](https://docs.stripe.com/api/charges/object) | целое, minor units | `integer` |
| [Adyen](https://docs.adyen.com/development-resources/currency-codes/) | minor units | integer |
| [Square](https://developer.squareup.com/reference/square/objects/Money) | smallest denomination | `int64` |
| [Google `Money`](https://developers.google.com/android-publisher/api-ref/rest/v3/Money) | `units` (int64) + `nanos` (int32) | строка + integer |
| [PayPal](https://raw.githubusercontent.com/paypal/paypal-rest-api-specifications/main/openapi/checkout_orders_v2.json) | десятичное | **строка** с regex |
| [Braintree](https://developer.paypal.com/braintree/docs/reference/request/transaction/sale) | `BigDecimal` | десятичное/строка |
| [Shopify](https://shopify.dev/docs/api/admin-graphql/latest/scalars/Decimal) | скаляр `Decimal` | **строка** |
| [Wise](https://github.com/transferwise/api-docs/blob/master/source/includes/reference/_quotes.md) | `Decimal` | число |
| [Plaid](https://raw.githubusercontent.com/plaid/plaid-openapi/master/2020-09-14.yml) | **`number` / `format: double`** | float64 |
| [T-Bank](https://developer.tbank.ru/eacq/api/init) | целое, копейки | `Integer<int64>` |
| [ЮKassa](https://yookassa.ru/developers/using-api/response-handling/response-format) | десятичное | строка |
| [ISO 20022](https://github.com/kedder/ofxstatement-iso20022/blob/master/doc/camt.053.001.05.xsd) | `xs:decimal(18,5)` + `Ccy` | XML decimal |

Stripe, дословно:

> «Amount intended to be collected by this payment. **A positive integer representing how
> much to charge in the smallest currency unit** (e.g., 100 cents to charge $1.00 or 100 to
> charge ¥100, a zero-decimal currency).»

Adyen:

> «our APIs expect you to submit transaction amount values in **minor units**: the smallest
> unit of a currency» — GBP 10 → `1000`, JPY 10 → `10`, BHD 10 → `10000`.

Plaid — единственный крупный игрок, кто честно везёт IEEE-754 double в публичной OpenAPI:

```yaml
amount:
  type: number
  format: double
```

### 5.2. Почему в API целые: причина в JSON

[RFC 8259, §6 «Numbers»](https://www.rfc-editor.org/rfc/rfc8259):

> «Since software that implements IEEE 754 binary64 (double precision) numbers is generally
> available and widely used, good interoperability can be achieved by implementations that
> expect no more precision or range than these provide…»
>
> «…numbers that are integers and are in the range [−(2**53)+1, (2**53)−1] are
> **interoperable** in the sense that implementations will agree exactly on their numeric
> values.»

В JSON нет десятичного типа, и гарантированно переживают round-trip только целые в
пределах ±2⁵³. Отсюда две стратегии: либо целые в minor units (Stripe, Adyen, T-Bank),
либо десятичное **строкой** (PayPal, Shopify, ЮKassa). Голое дробное JSON-число — это
третий, наименее надёжный путь.

Явная формулировка проблемы —
[тред api-craft «Encoding of money amounts in JSON representations»](https://groups.google.com/g/api-craft/c/jwnVh9TJyVw)
и [разбор про JS](https://cardinalby.github.io/blog/post/best-practices/storing-currency-values-data-types/):

> «JavaScript uses signed *Float64* as an internal representation for the *number* data
> type… by default a JavaScript application will overflow its number type trying to
> deserialize JSON containing this value.»

### 5.3. Ledger-системы

[Modern Treasury, «Floats don't work for storing cents»](https://www.moderntreasury.com/journal/floats-dont-work-for-storing-cents):

> «We primarily use 64-bit Integers to represent money in our system.»

Суммы хранятся в дробных единицах на **PostgreSQL `bigint`**.

Дословно противоположная позиция, для баланса —
[Otar Chekurishvili, «Storing money as integer cents is often over-engineering»](https://world.hey.com/otar/storing-money-as-integer-cents-is-often-over-engineering-7238a485):

> «When you store 1999 instead of 19.99, you don't actually solve a problem. You move it out
> of the database and into every other layer of the app.»

---

## 6. ERP и системы учёта: тут decimal побеждает

**SAP** — packed decimal плюс обязательное поле валюты
([ABAP docs, currency field](https://eduardocopat.github.io/abap-docs/7.31/abencurrency_field/);
оригинал `help.sap.com` закрыт robots.txt):

> «A data element of data type CURR is treated as a field of data type DEC and is stored in
> database tables in the BCD format.»
>
> «For every structure component of data type CURR, a component of the same structure or of
> a different structure or database table must be specified… as a reference field, which has
> the data type CUKY.»

Это чистейшая реализация паттерна Фаулера: сумма + обязательная ссылка на валюту.

**Oracle E-Business Suite / Fusion** — `NUMBER` вообще без точности и масштаба.
[GL_JE_LINES](https://docs.oracle.com/en/cloud/saas/financials/26a/oedmf/gljelines-24789.html):
`ENTERED_DR`, `ENTERED_CR`, `ACCOUNTED_DR`, `ACCOUNTED_CR` — все `NUMBER`.

**ERPNext / Frappe** — `decimal(21,9)` на обоих бэкендах
([mariadb/database.py](https://raw.githubusercontent.com/frappe/frappe/develop/frappe/database/mariadb/database.py),
postgres-версия идентична):

```python
"Currency": ("decimal", "21,9"),
"Float":    ("decimal", "21,9"),
"Percent":  ("decimal", "21,9"),
```

Девять знаков дробной части, и бинарных float-колонок нет вовсе.

**Odoo — лучший контрпример из всех, и не тот, которого ждёшь.** Колонка в базе —
`numeric`, [odoo/fields.py 17.0](https://raw.githubusercontent.com/odoo/odoo/17.0/odoo/fields.py):

```python
class Monetary(Field):
    """ Encapsulates a :class:`float` expressed in a given currency. """
    type = 'monetary'
    column_type = ('numeric', 'numeric')
```

Но значение в ORM — Python `float`. Оттуда и хелперы
([odoo/tools/float_utils.py](https://raw.githubusercontent.com/odoo/odoo/17.0/odoo/tools/float_utils.py)):

> `float_round`: «Return `value` rounded to `precision_digits` decimal digits, **minimizing
> IEEE-754 floating point representation errors**, and applying the tie-breaking rule
> selected with `rounding_method`, by default HALF-UP (away from zero).»
>
> `float_compare`: «**Warning: `float_is_zero(value1-value2)` is not equivalent to
> `float_compare(value1,value2) == 0`**, as the former will round after computing the
> difference, while the latter will round before…»

То есть production-ERP с `numeric`-колонками, которая всю арифметику ведёт в двоичном
double. **Колонка типа `numeric` сама по себе не означает десятичной арифметики** — это,
пожалуй, главный практический вывод всего раздела.

Версии Odoo ≤ 15.0 несут `column_cast_from = ('float8',)` — свидетельство, что колонка
исторически была `float8` и её мигрировали.

---

## 7. Что говорят практики PostgreSQL

[Wiki «Don't Do This»](https://wiki.postgresql.org/wiki/Don%27t_Do_This):

> «The money data type isn't actually very good for storing monetary values. **Numeric, or
> (rarely) integer may be better.**»
>
> «it doesn't handle fractions of a cent…, it's rounding behaviour is probably not what you
> want.»
>
> «if you insert '$10.00' while lc_monetary is set to 'en_US.UTF-8' the value you retrieve
> may be '10,00 Lei' or '¥1,000' if lc_monetary is changed.»
>
> «Storing a value as a numeric, possibly with the currency being used in an adjacent
> column, might be better.»

[Cybertec, Ханс-Юрген Шёниг](https://www.cybertec-postgresql.com/en/postgresql-int4-vs-float4-vs-numeric/):

> «In the case of money, different rounding rules are needed, which is why numeric is the
> data type you have to use to handle financial data.»

[Crunchy Data, Элизабет Кристенсен](https://www.crunchydata.com/blog/working-with-money-in-postgres) —
и обратите внимание на порядок:

> «Use `int` or `bigint` if you can work with whole numbers of cents and you don't need
> fractional cents.»
>
> «Use `decimal` / `numeric` for storing money in fractional cents and even out to many many
> decimal points.»
>
> «Store currency separately from the actual monetary values…»

---

## 8. Выводы

1. **Формального стандарта «для денег используйте NUMERIC» не существует.** В ISO SQL нет
   даже типа MONEY. Утверждать обратное — ошибка.
2. **Зато есть требования к поведению**, которым двоичная плавающая точка не удовлетворяет:
   точное представление копейки, детерминированное поведение на ровно половине, округление
   строго в предписанных точках и нигде больше, масштаб как параметр валюты. Это следует из
   права (регламент ЕС 1103/97, HMRC, п. 6 ст. 52 НК РФ + ПП № 1137), форматов обмена
   (ISO 20022, XBRL, FIX/SBE, ФФД, форматы ФНС) и транзакционных бенчмарков (TPC-C, TPC-E).
3. **Реальный консенсус звучит не «используйте decimal», а «никогда не двоичный float, и
   всегда валюта рядом с суммой».** Именно так формулируют его Фаулер, Блох, JSR 354 и
   PostgreSQL wiki.
4. **Внутри этого консенсуса две легитимные кодировки, и индустрия делится предсказуемо.**
   Системы учёта (SAP, Oracle EBS, ERPNext, ISO 20022, ЭДО-форматы ФНС, DirectBank) берут
   точное десятичное, потому что им нужен переменный и большой масштаб. Транспорт и
   высоконагруженные ledger'ы (Stripe, Adyen, T-Bank, ФФД, Modern Treasury) берут целые в
   minor units, потому что так удобнее JSON и процессору.
5. **Для PostgreSQL это значит:** `numeric` — правильный дефолт для системы учёта, и
   документация говорит это прямо. Но там же, в соседнем предложении, сказано «very slow
   compared to the integer types» — и `bigint` в копейках не ересь, а вторая половина того
   же индустриального консенсуса. Выбор между ними — про масштаб и про то, где проходит
   граница системы, а не про «правильно/неправильно».
6. **Отдельно и важно:** тип колонки не определяет тип арифметики. Odoo хранит в `numeric`
   и считает в `double`. Проверять надо оба слоя.

---

## 9. Что осталось непроверенным

- **Нормативный текст ISO/IEC 9075-2** — платный, проверен только по публичному SQL-92,
  вторичным источникам и грамматике SQL:2016.
- **Положение Банка России № 809-П** — дословную норму «в рублях и копейках» получить не
  удалось; подтверждено по предшественнику 579-П.
- **Положение № 762-П** — реквизиты цитируются по вторичным источникам; норма изменена
  Указанием от 17.06.2025, нумерация могла поехать.
- **Альбом УФЭБС Банка России** — скачать не удалось; тип атрибута `Sum` подтверждается
  только косвенно, по коду интеграторов.
- **XSD форматов ФНС** — читалась через инструмент-пересказчик; распределение конкретных
  атрибутов по масштабам (26.11 vs 19.2) требует сверки по скачанному файлу.
- **Спецификация СБП** — публичной нет; «копейки» выведены из примеров интеграторов.
- **Маппинг типов 1С в СУБД** — авторитетного источника не найдено.
- **Байтовые размеры DECFLOAT в документации IBM** — страницы `ibm.com/docs` рендерятся
  скриптом; значения 8/16 байт выведены из параметров формата IEEE.
- **Абсолютные цифры из mssqltips 3323** — только внутри картинок.
- **Позиция Christophe Pettus, depesz, Laurenz Albe по типам для денег** — содержательных
  публикаций найти не удалось.
- **Инструкции IRS по округлению до целого доллара** — страница не открылась.
- **Многоязычный поиск** (`polyglot-search`) в этой сессии не подключился; русскоязычные
  источники искались встроенным поиском и прямыми URL.
