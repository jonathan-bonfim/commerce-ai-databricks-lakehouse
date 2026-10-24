# Gold Validation, Known Exceptions, and Operations

**Verification source:** owner-reported execution of `10_gold_validation` and relevant business queries. This document is a review of reported results and previously provided notebook logic, **not** a fresh execution against the Databricks workspace.

## 1. Validated Gold gates

| Target | Gate checks | Observed outcome |
|---|---:|---|
| `dim_date` | 7 | PASS |
| `dim_customer` | 9 | PASS |
| `dim_product` | 8 | PASS |
| `dim_seller` | 9 | PASS |
| `fct_orders` | 12 | PASS |
| `fct_order_items` | 12 | PASS |
| `fct_payments` | 12 | PASS |
| `fct_reviews` | 13 | PASS |
| **Total** | **82** | **82 PASS / 0 FAIL** |

The gates check record preservation, business key uniqueness, Silver-to-Gold attribute fidelity, dimensional resolution, item/payment reconciliation by order and review multiplicity/text preservation as applicable. A successful `ASSERT_TRUE` produces a NULL result; it is not a null quality metric or a failed assertion.

**Qualification:** the 82-check total is the sum of the eight passed quality gates. An additional independent, all-table publication/version consistency transaction has **not** been demonstrated. The read-only business analysis queries are not quality gates and do not increase this count.

## 2. Known source exceptions intentionally retained

| Source/area | Observed condition | Known count | Gold handling |
|---|---|---:|---|
| Orders | Delivered status without actual delivery date | 8 | Preserve order; exclude from delivery-eligible denominator |
| Orders | Carrier date later than customer delivery date | 23 | Preserve and flag |
| Payments vs items | Payment value differs from item+freight total by more than BRL 0.01 | 303 orders | Preserve exact separate values; flag comparable mismatches |
| Payments vs items | Payments exist without items | 775 orders | Keep payments; order item amounts NULL, presence flag FALSE |
| Payments vs items | Items exist without payments | 1 order | Keep items; paid value NULL, presence flag FALSE |
| Payments | Zero-value payment records | 9 | Preserve and flag |
| Payments | Zero installment payment records | 2 | Preserve and flag |
| Products | Missing source category | 610 products | Preserve NULL and status `MISSING_CATEGORY` |
| Products | Source category without English translation | 13 products | Preserve NULL translation and status `UNTRANSLATED` |
| Customers / geolocation | Customer rows with no ZIP match | 278 | Keep dimension row; status `NOT_FOUND` |
| Sellers / geolocation | Seller rows with no ZIP match | 7 | Keep dimension row; status `NOT_FOUND` |
| Geolocation | ZIP prefixes without valid coordinate | 4 | Preserve nullable coordinates |
| Geolocation | ZIP prefixes with conflicting states | 8 | Preserve quality flags |
| Geolocation | ZIP prefixes with city variation | 8,555 | Preserve quality flags |
| Reviews | Orders with more than one review | 547 | Keep all reviews; composite business key |

These conditions are retained by design; they are not blanket approval for every future analytical use. Consumer metrics must specify denominator and missing-data treatment.

### Original Silver validation provenance

- Original integrated Silver notebook was **executed**: 83 checks, 69 PASS, 14 WARN, 0 FAIL (`PASS WITH WARNINGS`).
- A revised Silver notebook with 96 planned checks was prepared, but execution of that revised version was **not confirmed** in the supplied history.
- Gold outputs were validated against available Silver inputs and their known exceptions; this does not imply that the revised Silver 96-check gate was executed.

## 3. Data integrity and independent reconciliation

**Executed/observed findings:**

- `fct_order_items`: 112,650 rows; no duplicate composite business keys; item counts and values reconcile by `order_id` to `fct_orders`.
- `fct_payments`: 103,886 rows; no duplicate composite business keys; payment totals reconcile by `order_id` to `fct_orders`.
- `fct_reviews`: 99,224 rows; 98,673 orders with reviews; 547 with multiple rows; maximum three reviews per order. Known multiline quotation regression (`No "mercado livre" isso não acontece!!`) passed.
- `fct_orders`: 99,441 rows; 96,470 eligible deliveries; 6,534 historically late deliveries; late rate 6.77% with the documented date-only rule.

## 4. Operational risks / unverified capabilities

1. **Full rebuilds:** Gold tables are generated using independent `CREATE OR REPLACE TABLE ... USING DELTA AS` statements. Replacing eight tables in sequence is **not** one atomic transaction across the entire Gold layer; consumers could observe mixed table versions during rebuild.
2. **No proven end-to-end job:** no complete notebook orchestration with all dependencies, scheduling, retries, observability, or an automated rollback was confirmed.
3. **Quality gate is separate:** notebook `10_gold_validation.sql` must run **after** rebuilds. SQL temporary views and `ASSERT_TRUE` are session-scoped, so execute preparation/result/gate cells in the intended sequence rather than assuming old temporary views survive sessions.
4. **Historical static input:** the dataset has no documented incremental change data feed. Claims of real-time ingestion, Auto Loader, incremental SCD2 or continuously refreshed dashboards would be inaccurate.
5. **Consumption layer not deployed:** no Power BI relationships, row-level security, semantic refresh process, or direct Databricks integration has been demonstrated.
6. **Database timestamps:** the observed Spark session used `Etc/UTC`; original Olist source timestamps do not have an independently verified timezone.
7. **Repository language statistics:** native Databricks SQL Source notebooks should be versioned as `.sql` files in Git; GitHub Linguist stats depend on actual tracked source files and repository configuration, not only workspace notebook language.

## 5. Recommended pre-release checklist

- [ ] Confirm repository contains final notebook 08/09/10/11 versions and updated Gold documentation.
- [ ] Rebuild dimensions and facts in dependency order in a controlled test run.
- [ ] Execute the complete independent 82-check quality gate after the final Gold build.
- [ ] Compare baseline exceptions and counts against previous validated runs.
- [ ] Check whether the Gold tables were rebuilt against the same Silver input snapshot/version.
- [ ] Test a read-only business query across all intended consumption tables.
- [ ] Validate semantic model cardinalities and date roles before enabling dashboard refresh.
- [ ] Define atomic publication strategy if the project later requires concurrent consumers and recurring rebuilds.

Completing the checklist would be a **future operational milestone**. It is not required to describe the initial historical Gold model as implemented and validated, but it is required before claiming production-grade automated operation.
