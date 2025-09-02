from query_generator.tools.union_queries import get_select_list


def test_get_select_list():
  query = (
    "SELECT COUNT(*),COUNT(t.t_minute),COUNT(wp.wp_link_count),"
    "COUNT(c.c_birth_day),COUNT(wr.wr_net_loss) FROM time_dim t,web_page wp,"
    "customer c,web_returns wr WHERE wr.wr_returning_customer_sk=c.c_customer_sk"
    " AND wr.wr_returned_time_sk=t.t_time_sk AND wr.wr_web_page_sk="
    "wp.wp_web_page_sk AND wp.wp_image_count>=3 AND wp.wp_image_count<=5 "
    "AND t.t_meal_time IN ('dinner') AND wp.wp_creation_date_sk>=2450791 AND "
    "wp.wp_creation_date_sk<=2450805"
  )
  expected = (
    "COUNT(*),COUNT(t.t_minute),COUNT(wp.wp_link_count),"
    "COUNT(c.c_birth_day),COUNT(wr.wr_net_loss)"
  )
  print(query)
  assert get_select_list(query) == expected
