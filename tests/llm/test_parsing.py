import pytest

from query_generator.llm.llm_extension import extract_sql


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
  ],
)
def test_parser(llm_output, sql_query) -> None:
  parsed = extract_sql(llm_output)
  assert " ".join(parsed.split()) == " ".join(sql_query.split())
