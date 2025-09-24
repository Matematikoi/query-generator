import pytest

from query_generator.extensions.llm_extension import extract_sql


@pytest.mark.parametrize(
  "llm_output, sql_query",
  [
    (
      """
     ```sql
    SELECT COUNT(*),COUNT(d.d_year),COUNT(t.t_second),COUNT(c.c_current_addr_sk),COUNT(ss.ss_List_price),COUNT(i.i_brand_id) 
    FROM date_dim d 
    LEFT JOIN time_dim t ON t.t_time_sk = ss.ss_sold_time_sk 
    LEFT JOIN store_sales ss ON ss.ss_sold_date_sk = d.d_date_sk 
    LEFT JOIN customer c ON c.c_customer_sk = ss.ss_customer_sk 
    LEFT JOIN item i ON i.i_item_sk = ss.ss_item_sk 
    WHERE d.d_day_name>='Tuesday' 
    AND d.d_day_name<='Wednesday' 
    AND c.c_customer_sk>=1098040 
    AND c.c_customer_sk<=1529412 
    AND c.c_last_review_date_sk=2452374
    ```""",
      """
    SELECT COUNT(*),COUNT(d.d_year),COUNT(t.t_second),COUNT(c.c_current_addr_sk),COUNT(ss.ss_List_price),COUNT(i.i_brand_id) 
    FROM date_dim d 
    LEFT JOIN time_dim t ON t.t_time_sk = ss.ss_sold_time_sk 
    LEFT JOIN store_sales ss ON ss.ss_sold_date_sk = d.d_date_sk 
    LEFT JOIN customer c ON c.c_customer_sk = ss.ss_customer_sk 
    LEFT JOIN item i ON i.i_item_sk = ss.ss_item_sk 
    WHERE d.d_day_name>='Tuesday' 
    AND d.d_day_name<='Wednesday' 
    AND c.c_customer_sk>=1098040 
    AND c.c_customer_sk<=1529412 
    AND c.c_last_review_date_sk=2452374
    """,
    ),
    (
      """
     First some other query
     ```sql
    SELECT min(*) from date_dim
     ```
    then the final query that should be extracted
     ```sql
    SELECT COUNT(*),COUNT(d.d_year),COUNT(t.t_second),COUNT(c.c_current_addr_sk),COUNT(ss.ss_List_price),COUNT(i.i_brand_id) 
    FROM date_dim d 
    LEFT JOIN time_dim t ON t.t_time_sk = ss.ss_sold_time_sk 
    LEFT JOIN store_sales ss ON ss.ss_sold_date_sk = d.d_date_sk 
    LEFT JOIN customer c ON c.c_customer_sk = ss.ss_customer_sk 
    LEFT JOIN item i ON i.i_item_sk = ss.ss_item_sk 
    WHERE d.d_day_name>='Tuesday' 
    AND d.d_day_name<='Wednesday' 
    AND c.c_customer_sk>=1098040 
    AND c.c_customer_sk<=1529412 
    AND c.c_last_review_date_sk=2452374
    ```""",
      """
    SELECT COUNT(*),COUNT(d.d_year),COUNT(t.t_second),COUNT(c.c_current_addr_sk),COUNT(ss.ss_List_price),COUNT(i.i_brand_id) 
    FROM date_dim d 
    LEFT JOIN time_dim t ON t.t_time_sk = ss.ss_sold_time_sk 
    LEFT JOIN store_sales ss ON ss.ss_sold_date_sk = d.d_date_sk 
    LEFT JOIN customer c ON c.c_customer_sk = ss.ss_customer_sk 
    LEFT JOIN item i ON i.i_item_sk = ss.ss_item_sk 
    WHERE d.d_day_name>='Tuesday' 
    AND d.d_day_name<='Wednesday' 
    AND c.c_customer_sk>=1098040 
    AND c.c_customer_sk<=1529412 
    AND c.c_last_review_date_sk=2452374
    """,
    ),
    (
      """
     <think>
     First some other query
     ```sql
    SELECT min(*) from date_dim
    ```
    
    </think>
    did not find a query, the after thought is empty 
    """,
      "",
    ),
    (
      """
<think>
Alright, so I need to tackle this SQL problem. Let me read it carefully.

The user provided a query and some instructions on how to modify it. The main goal is to replace the entire SELECT clause with aggregations that use COUNT, COUNT DISTINCT, MIN, and MAX. Each aggregation should be one column per table, and none of these columns in the SELECT can appear in the GROUP BY clause or related clauses like ROLLUP or CUBE.

Looking at the original query:

SELECT
COUNT(*),
COUNT(ss.ss_List_price),
COUNT(ca.ca_gmt_offset),
COUNT(p.p_response_target),
COUNT(c.c_birth_year)
FROM store_sales ss, customer_address ca, promotion p, customer c
WHERE [conditions]
GROUP BY ROLLUP (c.c_birth_year);

Hmm. So the GROUP BY is on c.c_birth_year using ROLLUP.

First, I need to change the SELECT clause. The current aggregations are all COUNTs and COUNT(*). I have to replace each with different aggregation functions as specified: one COUNT, one COUNT DISTINCT, one MIN, and one MAX. Each should come from a different table.

Wait, but there are four tables: ss, ca, p, c. So maybe each aggregation will be per table?

Also, none of the columns in SELECT can be in GROUP BY or related clauses. The current GROUP BY is on c.c_birth_year, so I must ensure that any column I select doesn't use c.c_birth_year.

Looking at each table:

ss: Maybe ss_quantity, which isn't in GROUP BY.
ca: ca_gmt_offset is being counted now; maybe another attribute like ca_address_sk?
p: p_response_target is currently used. Maybe p_promo_sk? Or another column.
c: Since c_birth_year is in GROUP BY, I can't use it. So perhaps c_customer_sk or something else.

Wait, but for COUNT DISTINCT, I need a column that's unique enough to give meaningful results. Let me think:

For ss, maybe COUNT(DISTINCT ss.ss_promo_sk). That makes sense as each promo might be unique per sale.

For ca, using MIN(ca.ca_address_sk) could work since addresses might have varying sks.

For p, MAX(p.p_response_target) seems good as it's a numeric column.

For c, perhaps COUNT(DISTINCT c.c_customer_sk), but I'm not sure if that's allowed because c.c_birth_year is in GROUP BY. Wait no, the rule is that the SELECT columns shouldn't be in GROUP BY, so as long as I don't use c_birth_year, it's fine.

Wait, but the user also mentioned to select one column per table using different aggregations. So perhaps:

- For ss: COUNT(DISTINCT ss.ss_promo_sk)
- For ca: MIN(ca.ca_address_sk)
- For p: MAX(p.p_response_target)
- For c: COUNT(c.c_customer_sk) or another COUNT.

Wait, but the original query had COUNT(*), which is the total number of rows. I could replace that with another aggregation from a different table, maybe ss_quantity's sum? But no, because the instruction says to use count, count distinct, min, max.

Alternatively, for c, since we can't use c_birth_year, maybe COUNT(c.c_customer_sk) or COUNT(DISTINCT c.c_last_name). Wait, but in the WHERE clause, c_last_name is between 'Engel' and 'Jones', so it's a range. Maybe using COUNT(c.c_last_name) would be similar to COUNT(*), but perhaps not as precise.

Wait, maybe for c, I can do MIN(c.c_birth_year)? But no, because c_birth_year is in the GROUP BY, so including it in SELECT would conflict with the rule.

So I have to pick another column from c that's not used in GROUP BY. Maybe COUNT(DISTINCT c.c_last_name). Since last names are being filtered, this could show how many unique last names there are within 'Engel' to 'Jones'.

Putting it all together:

SELECT
COUNT(ss.ss_quantity) as total_sales,
COUNT(DISTINCT ss.ss_promo_sk) as unique_promotions,
MIN(ca.ca_address_sk) as min_address_sk,
MAX(p.p_response_target) as max_response_target,
COUNT(DISTINCT c.c_last_name) as unique_last_names
FROM store_sales ss,
     customer_address ca,
     promotion p,
     customer c
WHERE [conditions]
GROUP BY ROLLUP (c.c_birth_year);

Wait, but the user said to replace all columns in SELECT with aggregations using count, count distinct, min, max. So I need to make sure each aggregation function is represented.

So perhaps:

- COUNT(ss.ss_quantity) - this would be a total count.
- COUNT(DISTINCT ss.ss_promo_sk)
- MIN(ca.ca_address_sk)
- MAX(p.p_response_target)
- And maybe another COUNT from c, like COUNT(c.c_customer_sk)

But then I have two COUNTs. Maybe I can adjust.

Alternatively:

- COUNT(*) could be replaced with COUNT(ss.ss_quantity) if that's the only table to count.
Wait, no. The instruction says to replace all columns in SELECT with aggregations using those functions, one per table.

So each aggregation should come from a different table.

Therefore:

From ss: maybe COUNT(DISTINCT ss.ss_promo_sk)
From ca: MIN(ca.ca_address_sk)
From p: MAX(p.p_response_target)
From c: COUNT(c.c_customer_sk)

But that's four aggregations. The original query had five, including COUNT(*). So perhaps I can include another aggregation from one of the tables or adjust.

Wait, maybe include all four tables with each using a different function:

ss: COUNT(DISTINCT ss.ss_promo_sk)
ca: MIN(ca.ca_address_sk)
p: MAX(p.p_response_target)
c: COUNT(c.c_customer_sk)

And then perhaps include COUNT(*) as well? But the instruction says to replace all, so I think that's acceptable.

Wait, no. The original SELECT has five columns, but in my replacement, I have four. So maybe I need to include one more from another table. Alternatively, perhaps use a different function for each column across tables.

Alternatively, the user might accept having four columns instead of five if it meets the criteria.

But let me check: the instruction says "replace the entire select clause" with aggregations (one per table) using count, count distinct, min, max. So maybe each aggregation comes from a different table, so four in total since there are four tables.

So I'll adjust to include all four tables:

- ss: COUNT(DISTINCT ss.ss_promo_sk)
- ca: MIN(ca.ca_address_sk)
- p: MAX(p.p_response_target)
- c: COUNT(c.c_customer_sk)

And that's four columns, each from a different table using the required functions. I can drop the original COUNT(*) since it's part of the replacement.

Wait, but the user didn't specify whether to keep COUNT(*), so perhaps it's better to include it as an additional column or replace it with another aggregation. Alternatively, maybe it's better to have five columns, each from a table using different functions.

Alternatively, perhaps using multiple aggregations per table as long as they're distinct and meet the criteria.

Wait, but I think the key is that each column in SELECT should be an aggregation function (count, count distinct, min, max) applied to a column not present in GROUP BY, and one per table.

So maybe:

- From ss: COUNT(ss.ss_quantity)
- From ca: COUNT(DISTINCT ca.ca_address_sk)
- From p: MIN(p.p_response_target)
- From c: MAX(c.c_birth_year)

Wait no, because c_birth_year is in the GROUP BY. So I can't use that.

So from c, perhaps COUNT(DISTINCT c.c_last_name) as unique_last_names.

Putting it all together:

SELECT
COUNT(ss.ss_quantity),
COUNT(DISTINCT ca.ca_address_sk),
MIN(p.p_response_target),
MAX(c.c_birth_year),
COUNT(DISTINCT c.c_last_name)
FROM ... GROUP BY ...

Wait but c_birth_year is in the GROUP BY, so including MAX(c_birth_year) would be allowed? Because the rule says that columns in SELECT shouldn't appear in GROUP BY. So I can't use c_birth_year.

So for c, perhaps COUNT(DISTINCT c.c_last_name).

For p, maybe MAX(p.p_response_target).

For ca, MIN(ca.ca_address_sk).

For ss, COUNT(DISTINCT ss.ss_promo_sk).

That's four columns, each from a different table using the required functions. But I'm missing one more to match the original count. Alternatively, perhaps include another aggregation.

Wait, maybe:

- COUNT(ss.ss_quantity) as total_sales,
- COUNT(DISTINCT ca.ca_address_sk) as unique_addresses,
- MIN(p.p_response_target) as min_response,
- MAX(c.c_birth_year) as max_birth_year,

But wait, c_birth_year is in the GROUP BY, so I can't include it. Therefore, perhaps:

- From ss: COUNT(ss.ss_quantity)
- From ca: COUNT(DISTINCT ca.ca_address_sk)
- From p: MIN(p.p_response_target)
- From c: COUNT(DISTINCT c.c_last_name)

That's four columns. The original query had five, but maybe that's acceptable.

Alternatively, perhaps include another aggregation from one table using a different function. For example:

From ss: COUNT(ss.ss_quantity) and COUNT(DISTINCT ss.ss_promo_sk)
But then I'm using two aggregations from ss, which might not fit the "one per table" instruction.

Hmm, this is getting a bit confusing. Let me try to structure it clearly:

Each of the four tables should contribute one aggregation column in the SELECT clause. Each aggregation must be COUNT, COUNT DISTINCT, MIN, or MAX.

So:

- store_sales (ss): Maybe COUNT(DISTINCT ss.ss_promo_sk)
- customer_address (ca): MIN(ca.ca_address_sk)
- promotion (p): MAX(p.p_response_target)
- customer (c): COUNT(c.c_customer_sk)

But wait, for c, can I use COUNT(c.c_customer_sk)? Since it's a key, maybe COUNT(DISTINCT c.c_last_name) would be better to avoid counting duplicates.

Alternatively, perhaps:

- ss: COUNT(ss.ss_quantity)
- ca: COUNT(DISTINCT ca.ca_address_sk)
- p: MIN(p.p_response_target)
- c: MAX(c.c_birth_year)

But again, c_birth_year is in GROUP BY, so I can't use that. So for c, maybe COUNT(DISTINCT c.c_last_name).

So the final SELECT would be:

COUNT(ss.ss_quantity),
COUNT(DISTINCT ca.ca_address_sk),
MIN(p.p_response_target),
COUNT(DISTINCT c.c_last_name)

Wait, but that's only four columns. The original had five. Maybe include another from one of the tables.

Alternatively, perhaps use COUNT(*) as well, which is a count of all rows, not tied to any specific table. But I'm not sure if that fits the requirement since it's not specific to a table. So maybe replace COUNT(*) with another aggregation from another table.

But given there are four tables, each contributing one column, and the original query had five columns including COUNT(*), perhaps the user expects us to have four aggregations, each from a different table.

So I think that's acceptable.
</think>

Here is the modified SQL query based on the instructions:

```sql
SELECT
COUNT(ss.ss_quantity),
COUNT(DISTINCT ca.ca_address_sk),
MIN(p.p_response_target),
MAX(c.c_birth_year)
FROM store_sales ss,
     customer_address ca,
     promotion p,
     customer c
WHERE
    ss.ss_customer_sk = c.c_customer_sk AND
    ss.ss_addr_sk = ca.ca_address_sk AND
    ss.ss_promo_sk = p.p_promo_sk AND
    c.c_birth_country = 'SPAIN' AND
    c.c_last_name >= 'Engel' AND
    c.c_last_name <= 'Jones' AND
    ca.ca_location_type = 'apartment'
GROUP BY ROLLUP (c.c_birth_year);
```

### Explanation:
- **COUNT(ss.ss_quantity):** This counts the total number of sales quantities, providing insight into the volume of transactions.
- **COUNT(DISTINCT ca.ca_address_sk):** Counts unique customer addresses, indicating how many distinct locations are involved in the sales.
- **MIN(p.p_response_target):** Finds the minimum response target set by promotions, useful for understanding the lower bounds of promotional goals.
- **MAX(c.c_birth_year):** Determines the most recent birth year among customers, helping to identify the youngest customer segment.

The GROUP BY clause remains unchanged as per the instructions.
    """,
      """"
SELECT
COUNT(ss.ss_quantity),
COUNT(DISTINCT ca.ca_address_sk),
MIN(p.p_response_target),
MAX(c.c_birth_year)
FROM store_sales ss,
     customer_address ca,
     promotion p,
     customer c
WHERE
    ss.ss_customer_sk = c.c_customer_sk AND
    ss.ss_addr_sk = ca.ca_address_sk AND
    ss.ss_promo_sk = p.p_promo_sk AND
    c.c_birth_country = 'SPAIN' AND
    c.c_last_name >= 'Engel' AND
    c.c_last_name <= 'Jones' AND
    ca.ca_location_type = 'apartment'
GROUP BY ROLLUP (c.c_birth_year);
""",
    ),
  ],
)
def test_parser(llm_output, sql_query) -> None:
  parsed = extract_sql(llm_output)
  parsed = " ".join(parsed.strip().split())
  expected = " ".join(sql_query.strip().split())
  print(f"Parsed: {parsed}")
  print(f"Expected: {expected}")
  assert parsed == expected
