from enum import StrEnum
from pathlib import Path
from query_generator.utils.params import FixTransformEndpoint
from sqlglot import exp, parse_one
import polars as pl
from tqdm import tqdm
import random

class DuckDBTraceEnum(StrEnum):
    """Rows for DuckDBTraceOuputDataFrameRow."""

    relative_path = "relative_path"
    query_folder = "query_folder"
    query_name = "query_name"
    duckdb_trace = "duckdb_trace"
    error = "error"
    trace_success = "trace_success"
    duckdb_output = "duckdb_output"
    error_group_by_sqlglot = "error_group_by_sqlglot"
class TransformationCount(StrEnum):
    COUNT = "COUNT"
    MAX  = "MAX "
    MIN  = "MIN "
    DISTINCT = "DISTINCT"


CTE_NAME = "cte_for_limit"

tables = {'call_center': {'cc_call_center_sk': 'INT', 'cc_call_center_id': 'TEXT', 'cc_rec_start_date': 'DATE', 'cc_rec_end_date': 'DATE', 'cc_closed_date_sk': 'INT', 'cc_open_date_sk': 'INT', 'cc_name': 'TEXT', 'cc_class': 'TEXT', 'cc_employees': 'INT', 'cc_sq_ft': 'INT', 'cc_hours': 'TEXT', 'cc_manager': 'TEXT', 'cc_mkt_id': 'INT', 'cc_mkt_class': 'TEXT', 'cc_mkt_desc': 'TEXT', 'cc_market_manager': 'TEXT', 'cc_division': 'INT', 'cc_division_name': 'TEXT', 'cc_company': 'INT', 'cc_company_name': 'TEXT', 'cc_street_number': 'TEXT', 'cc_street_name': 'TEXT', 'cc_street_type': 'TEXT', 'cc_suite_number': 'TEXT', 'cc_city': 'TEXT', 'cc_county': 'TEXT', 'cc_state': 'TEXT', 'cc_zip': 'TEXT', 'cc_country': 'TEXT', 'cc_gmt_offset': 'DECIMAL(5, 2)', 'cc_tax_percentage': 'DECIMAL(5, 2)'}, 'catalog_page': {'cp_catalog_page_sk': 'INT', 'cp_catalog_page_id': 'TEXT', 'cp_start_date_sk': 'INT', 'cp_end_date_sk': 'INT', 'cp_department': 'TEXT', 'cp_catalog_number': 'INT', 'cp_catalog_page_number': 'INT', 'cp_description': 'TEXT', 'cp_type': 'TEXT'}, 'catalog_returns': {'cr_returned_time_sk': 'INT', 'cr_item_sk': 'INT', 'cr_refunded_customer_sk': 'INT', 'cr_refunded_cdemo_sk': 'INT', 'cr_refunded_hdemo_sk': 'INT', 'cr_refunded_addr_sk': 'INT', 'cr_returning_customer_sk': 'INT', 'cr_returning_cdemo_sk': 'INT', 'cr_returning_hdemo_sk': 'INT', 'cr_returning_addr_sk': 'INT', 'cr_call_center_sk': 'INT', 'cr_catalog_page_sk': 'INT', 'cr_ship_mode_sk': 'INT', 'cr_warehouse_sk': 'INT', 'cr_reason_sk': 'INT', 'cr_order_number': 'BIGINT', 'cr_return_quantity': 'INT', 'cr_return_amount': 'DECIMAL(7, 2)', 'cr_return_tax': 'DECIMAL(7, 2)', 'cr_return_amt_inc_tax': 'DECIMAL(7, 2)', 'cr_fee': 'DECIMAL(7, 2)', 'cr_return_ship_cost': 'DECIMAL(7, 2)', 'cr_refunded_cash': 'DECIMAL(7, 2)', 'cr_reversed_charge': 'DECIMAL(7, 2)', 'cr_store_credit': 'DECIMAL(7, 2)', 'cr_net_loss': 'DECIMAL(7, 2)', 'cr_returned_date_sk': 'INT'}, 'catalog_sales': {'cs_sold_time_sk': 'INT', 'cs_ship_date_sk': 'INT', 'cs_bill_customer_sk': 'INT', 'cs_bill_cdemo_sk': 'INT', 'cs_bill_hdemo_sk': 'INT', 'cs_bill_addr_sk': 'INT', 'cs_ship_customer_sk': 'INT', 'cs_ship_cdemo_sk': 'INT', 'cs_ship_hdemo_sk': 'INT', 'cs_ship_addr_sk': 'INT', 'cs_call_center_sk': 'INT', 'cs_catalog_page_sk': 'INT', 'cs_ship_mode_sk': 'INT', 'cs_warehouse_sk': 'INT', 'cs_item_sk': 'INT', 'cs_promo_sk': 'INT', 'cs_order_number': 'BIGINT', 'cs_quantity': 'INT', 'cs_wholesale_cost': 'DECIMAL(7, 2)', 'cs_list_price': 'DECIMAL(7, 2)', 'cs_sales_price': 'DECIMAL(7, 2)', 'cs_ext_discount_amt': 'DECIMAL(7, 2)', 'cs_ext_sales_price': 'DECIMAL(7, 2)', 'cs_ext_wholesale_cost': 'DECIMAL(7, 2)', 'cs_ext_list_price': 'DECIMAL(7, 2)', 'cs_ext_tax': 'DECIMAL(7, 2)', 'cs_coupon_amt': 'DECIMAL(7, 2)', 'cs_ext_ship_cost': 'DECIMAL(7, 2)', 'cs_net_paid': 'DECIMAL(7, 2)', 'cs_net_paid_inc_tax': 'DECIMAL(7, 2)', 'cs_net_paid_inc_ship': 'DECIMAL(7, 2)', 'cs_net_paid_inc_ship_tax': 'DECIMAL(7, 2)', 'cs_net_profit': 'DECIMAL(7, 2)', 'cs_sold_date_sk': 'INT'}, 'customer': {'c_customer_sk': 'INT', 'c_customer_id': 'TEXT', 'c_current_cdemo_sk': 'INT', 'c_current_hdemo_sk': 'INT', 'c_current_addr_sk': 'INT', 'c_first_shipto_date_sk': 'INT', 'c_first_sales_date_sk': 'INT', 'c_salutation': 'TEXT', 'c_first_name': 'TEXT', 'c_last_name': 'TEXT', 'c_preferred_cust_flag': 'TEXT', 'c_birth_day': 'INT', 'c_birth_month': 'INT', 'c_birth_year': 'INT', 'c_birth_country': 'TEXT', 'c_login': 'TEXT', 'c_email_address': 'TEXT', 'c_last_review_date': 'TEXT'}, 'customer_address': {'ca_address_sk': 'INT', 'ca_address_id': 'TEXT', 'ca_street_number': 'TEXT', 'ca_street_name': 'TEXT', 'ca_street_type': 'TEXT', 'ca_suite_number': 'TEXT', 'ca_city': 'TEXT', 'ca_county': 'TEXT', 'ca_state': 'TEXT', 'ca_zip': 'TEXT', 'ca_country': 'TEXT', 'ca_gmt_offset': 'DECIMAL(5, 2)', 'ca_location_type': 'TEXT'}, 'customer_demographics': {'cd_demo_sk': 'INT', 'cd_gender': 'TEXT', 'cd_marital_status': 'TEXT', 'cd_education_status': 'TEXT', 'cd_purchase_estimate': 'INT', 'cd_credit_rating': 'TEXT', 'cd_dep_count': 'INT', 'cd_dep_employed_count': 'INT', 'cd_dep_college_count': 'INT'}, 'date_dim': {'d_date_sk': 'INT', 'd_date_id': 'TEXT', 'd_date': 'DATE', 'd_month_seq': 'INT', 'd_week_seq': 'INT', 'd_quarter_seq': 'INT', 'd_year': 'INT', 'd_dow': 'INT', 'd_moy': 'INT', 'd_dom': 'INT', 'd_qoy': 'INT', 'd_fy_year': 'INT', 'd_fy_quarter_seq': 'INT', 'd_fy_week_seq': 'INT', 'd_day_name': 'TEXT', 'd_quarter_name': 'TEXT', 'd_holiday': 'TEXT', 'd_weekend': 'TEXT', 'd_following_holiday': 'TEXT', 'd_first_dom': 'INT', 'd_last_dom': 'INT', 'd_same_day_ly': 'INT', 'd_same_day_lq': 'INT', 'd_current_day': 'TEXT', 'd_current_week': 'TEXT', 'd_current_month': 'TEXT', 'd_current_quarter': 'TEXT', 'd_current_year': 'TEXT'}, 'household_demographics': {'hd_demo_sk': 'INT', 'hd_income_band_sk': 'INT', 'hd_buy_potential': 'TEXT', 'hd_dep_count': 'INT', 'hd_vehicle_count': 'INT'}, 'income_band': {'ib_income_band_sk': 'INT', 'ib_lower_bound': 'INT', 'ib_upper_bound': 'INT'}, 'inventory': {'inv_item_sk': 'INT', 'inv_warehouse_sk': 'INT', 'inv_quantity_on_hand': 'INT', 'inv_date_sk': 'INT'}, 'item': {'i_item_sk': 'INT', 'i_item_id': 'TEXT', 'i_rec_start_date': 'DATE', 'i_rec_end_date': 'DATE', 'i_item_desc': 'TEXT', 'i_current_price': 'DECIMAL(7, 2)', 'i_wholesale_cost': 'DECIMAL(7, 2)', 'i_brand_id': 'INT', 'i_brand': 'TEXT', 'i_class_id': 'INT', 'i_class': 'TEXT', 'i_category_id': 'INT', 'i_category': 'TEXT', 'i_manufact_id': 'INT', 'i_manufact': 'TEXT', 'i_size': 'TEXT', 'i_formulation': 'TEXT', 'i_color': 'TEXT', 'i_units': 'TEXT', 'i_container': 'TEXT', 'i_manager_id': 'INT', 'i_product_name': 'TEXT'}, 'promotion': {'p_promo_sk': 'INT', 'p_promo_id': 'TEXT', 'p_start_date_sk': 'INT', 'p_end_date_sk': 'INT', 'p_item_sk': 'INT', 'p_cost': 'DECIMAL(15, 2)', 'p_response_target': 'INT', 'p_promo_name': 'TEXT', 'p_channel_dmail': 'TEXT', 'p_channel_email': 'TEXT', 'p_channel_catalog': 'TEXT', 'p_channel_tv': 'TEXT', 'p_channel_radio': 'TEXT', 'p_channel_press': 'TEXT', 'p_channel_event': 'TEXT', 'p_channel_demo': 'TEXT', 'p_channel_details': 'TEXT', 'p_purpose': 'TEXT', 'p_discount_active': 'TEXT'}, 'reason': {'r_reason_sk': 'INT', 'r_reason_id': 'TEXT', 'r_reason_desc': 'TEXT'}, 'ship_mode': {'sm_ship_mode_sk': 'INT', 'sm_ship_mode_id': 'TEXT', 'sm_type': 'TEXT', 'sm_code': 'TEXT', 'sm_carrier': 'TEXT', 'sm_contract': 'TEXT'}, 'store': {'s_store_sk': 'INT', 's_store_id': 'TEXT', 's_rec_start_date': 'DATE', 's_rec_end_date': 'DATE', 's_closed_date_sk': 'INT', 's_store_name': 'TEXT', 's_number_employees': 'INT', 's_floor_space': 'INT', 's_hours': 'TEXT', 's_manager': 'TEXT', 's_market_id': 'INT', 's_geography_class': 'TEXT', 's_market_desc': 'TEXT', 's_market_manager': 'TEXT', 's_division_id': 'INT', 's_division_name': 'TEXT', 's_company_id': 'INT', 's_company_name': 'TEXT', 's_street_number': 'TEXT', 's_street_name': 'TEXT', 's_street_type': 'TEXT', 's_suite_number': 'TEXT', 's_city': 'TEXT', 's_county': 'TEXT', 's_state': 'TEXT', 's_zip': 'TEXT', 's_country': 'TEXT', 's_gmt_offset': 'DECIMAL(5, 2)', 's_tax_precentage': 'DECIMAL(5, 2)'}, 'store_returns': {'sr_return_time_sk': 'INT', 'sr_item_sk': 'INT', 'sr_customer_sk': 'INT', 'sr_cdemo_sk': 'INT', 'sr_hdemo_sk': 'INT', 'sr_addr_sk': 'INT', 'sr_store_sk': 'INT', 'sr_reason_sk': 'INT', 'sr_ticket_number': 'BIGINT', 'sr_return_quantity': 'INT', 'sr_return_amt': 'DECIMAL(7, 2)', 'sr_return_tax': 'DECIMAL(7, 2)', 'sr_return_amt_inc_tax': 'DECIMAL(7, 2)', 'sr_fee': 'DECIMAL(7, 2)', 'sr_return_ship_cost': 'DECIMAL(7, 2)', 'sr_refunded_cash': 'DECIMAL(7, 2)', 'sr_reversed_charge': 'DECIMAL(7, 2)', 'sr_store_credit': 'DECIMAL(7, 2)', 'sr_net_loss': 'DECIMAL(7, 2)', 'sr_returned_date_sk': 'INT'}, 'store_sales': {'ss_sold_time_sk': 'INT', 'ss_item_sk': 'INT', 'ss_customer_sk': 'INT', 'ss_cdemo_sk': 'INT', 'ss_hdemo_sk': 'INT', 'ss_addr_sk': 'INT', 'ss_store_sk': 'INT', 'ss_promo_sk': 'INT', 'ss_ticket_number': 'BIGINT', 'ss_quantity': 'INT', 'ss_wholesale_cost': 'DECIMAL(7, 2)', 'ss_list_price': 'DECIMAL(7, 2)', 'ss_sales_price': 'DECIMAL(7, 2)', 'ss_ext_discount_amt': 'DECIMAL(7, 2)', 'ss_ext_sales_price': 'DECIMAL(7, 2)', 'ss_ext_wholesale_cost': 'DECIMAL(7, 2)', 'ss_ext_list_price': 'DECIMAL(7, 2)', 'ss_ext_tax': 'DECIMAL(7, 2)', 'ss_coupon_amt': 'DECIMAL(7, 2)', 'ss_net_paid': 'DECIMAL(7, 2)', 'ss_net_paid_inc_tax': 'DECIMAL(7, 2)', 'ss_net_profit': 'DECIMAL(7, 2)', 'ss_sold_date_sk': 'INT'}, 'time_dim': {'t_time_sk': 'INT', 't_time_id': 'TEXT', 't_time': 'INT', 't_hour': 'INT', 't_minute': 'INT', 't_second': 'INT', 't_am_pm': 'TEXT', 't_shift': 'TEXT', 't_sub_shift': 'TEXT', 't_meal_time': 'TEXT'}, 'warehouse': {'w_warehouse_sk': 'INT', 'w_warehouse_id': 'TEXT', 'w_warehouse_name': 'TEXT', 'w_warehouse_sq_ft': 'INT', 'w_street_number': 'TEXT', 'w_street_name': 'TEXT', 'w_street_type': 'TEXT', 'w_suite_number': 'TEXT', 'w_city': 'TEXT', 'w_county': 'TEXT', 'w_state': 'TEXT', 'w_zip': 'TEXT', 'w_country': 'TEXT', 'w_gmt_offset': 'DECIMAL(5, 2)'}, 'web_returns': {'wr_returned_time_sk': 'INT', 'wr_item_sk': 'INT', 'wr_refunded_customer_sk': 'INT', 'wr_refunded_cdemo_sk': 'INT', 'wr_refunded_hdemo_sk': 'INT', 'wr_refunded_addr_sk': 'INT', 'wr_returning_customer_sk': 'INT', 'wr_returning_cdemo_sk': 'INT', 'wr_returning_hdemo_sk': 'INT', 'wr_returning_addr_sk': 'INT', 'wr_web_page_sk': 'INT', 'wr_reason_sk': 'INT', 'wr_order_number': 'BIGINT', 'wr_return_quantity': 'INT', 'wr_return_amt': 'DECIMAL(7, 2)', 'wr_return_tax': 'DECIMAL(7, 2)', 'wr_return_amt_inc_tax': 'DECIMAL(7, 2)', 'wr_fee': 'DECIMAL(7, 2)', 'wr_return_ship_cost': 'DECIMAL(7, 2)', 'wr_refunded_cash': 'DECIMAL(7, 2)', 'wr_reversed_charge': 'DECIMAL(7, 2)', 'wr_account_credit': 'DECIMAL(7, 2)', 'wr_net_loss': 'DECIMAL(7, 2)', 'wr_returned_date_sk': 'INT'}, 'web_sales': {'ws_sold_time_sk': 'INT', 'ws_ship_date_sk': 'INT', 'ws_item_sk': 'INT', 'ws_bill_customer_sk': 'INT', 'ws_bill_cdemo_sk': 'INT', 'ws_bill_hdemo_sk': 'INT', 'ws_bill_addr_sk': 'INT', 'ws_ship_customer_sk': 'INT', 'ws_ship_cdemo_sk': 'INT', 'ws_ship_hdemo_sk': 'INT', 'ws_ship_addr_sk': 'INT', 'ws_web_page_sk': 'INT', 'ws_web_site_sk': 'INT', 'ws_ship_mode_sk': 'INT', 'ws_warehouse_sk': 'INT', 'ws_promo_sk': 'INT', 'ws_order_number': 'BIGINT', 'ws_quantity': 'INT', 'ws_wholesale_cost': 'DECIMAL(7, 2)', 'ws_list_price': 'DECIMAL(7, 2)', 'ws_sales_price': 'DECIMAL(7, 2)', 'ws_ext_discount_amt': 'DECIMAL(7, 2)', 'ws_ext_sales_price': 'DECIMAL(7, 2)', 'ws_ext_wholesale_cost': 'DECIMAL(7, 2)', 'ws_ext_list_price': 'DECIMAL(7, 2)', 'ws_ext_tax': 'DECIMAL(7, 2)', 'ws_coupon_amt': 'DECIMAL(7, 2)', 'ws_ext_ship_cost': 'DECIMAL(7, 2)', 'ws_net_paid': 'DECIMAL(7, 2)', 'ws_net_paid_inc_tax': 'DECIMAL(7, 2)', 'ws_net_paid_inc_ship': 'DECIMAL(7, 2)', 'ws_net_paid_inc_ship_tax': 'DECIMAL(7, 2)', 'ws_net_profit': 'DECIMAL(7, 2)', 'ws_sold_date_sk': 'INT'}, 'web_site': {'web_site_sk': 'INT', 'web_site_id': 'TEXT', 'web_rec_start_date': 'DATE', 'web_rec_end_date': 'DATE', 'web_name': 'TEXT', 'web_open_date_sk': 'INT', 'web_close_date_sk': 'INT', 'web_class': 'TEXT', 'web_manager': 'TEXT', 'web_mkt_id': 'INT', 'web_mkt_class': 'TEXT', 'web_mkt_desc': 'TEXT', 'web_market_manager': 'TEXT', 'web_company_id': 'INT', 'web_company_name': 'TEXT', 'web_street_number': 'TEXT', 'web_street_name': 'TEXT', 'web_street_type': 'TEXT', 'web_suite_number': 'TEXT', 'web_city': 'TEXT', 'web_county': 'TEXT', 'web_state': 'TEXT', 'web_zip': 'TEXT', 'web_country': 'TEXT', 'web_gmt_offset': 'DECIMAL(5, 2)', 'web_tax_percentage': 'DECIMAL(5, 2)'}, 'web_page': {'wp_web_page_sk': 'INT', 'wp_web_page_id': 'TEXT', 'wp_rec_start_date': 'DATE', 'wp_rec_end_date': 'DATE', 'wp_creation_date_sk': 'INT', 'wp_access_date_sk': 'INT', 'wp_autogen_flag': 'TEXT', 'wp_customer_sk': 'INT', 'wp_url': 'TEXT', 'wp_type': 'TEXT', 'wp_char_count': 'INT', 'wp_link_count': 'INT', 'wp_image_count': 'INT', 'wp_max_ad_count': 'INT'}}

def wrap_query_with_limit(sql:str, limit:int) -> str:
    original: exp.Expression = parse_one(sql)

    cte_alias = exp.TableAlias(this=exp.to_identifier(CTE_NAME))
    cte = exp.CTE(this=original.copy(), alias=cte_alias)
    with_clause = exp.With(expressions=[cte])

    outer_select = (
        exp.select("*")
        .from_(exp.to_table(CTE_NAME))
        .limit(limit)
    )

    outer_select.set("with", with_clause)

    return outer_select.sql(pretty=True)

def get_only_columns_in_select(tree):
    cols = []
    select = tree.find(exp.Select)
    if select:
        for proj in select.expressions or []:
            for col in proj.find_all(exp.Column):
                col_sql = col.sql()
                if "*" not in col_sql:
                    cols.append(col_sql)
    return cols

def get_group_by_attributes(tree):
    return [i.sql() for i in tree.find(exp.Group).find_all(exp.Column)]


def get_select_attributes(tree):
    return [i.sql() for i in tree.find(exp.Select) if "*" not in i.sql()]

def retrieve_column_name(table_dot_columns):
    result = []
    for t in table_dot_columns:
        if "." in table_dot_columns:
            result.append(t.split(".")[1])
        else:
            result.append(t)
    return result

def get_subquery(tree, column):
    select_values = get_select_attributes(tree)
    for select_value in select_values:
        if column in select_value:
            return select_value
    raise KeyError("Column not found in select attributes")

def get_table_from_column(col: str) -> str:
    search_col = col.split(".")[1] if "." in col else col
    for table_name in tables:
        if search_col.lower() in tables[table_name].keys():
            return table_name
    return None

def change_select_attribute(sql, sub_sql, new_column, old_column):
    if any(
        keyword in sub_sql.lower() for keyword in ["order by", "grouping(", " over "]
    ):
        return sql
    old_column = old_column.split(".")[1] if "." in old_column else old_column
    new_sub_sql = sub_sql.replace(old_column, new_column)
    return sql.replace(sub_sql, new_sub_sql)


def get_repeated_columns(tree):
    select_columns = get_only_columns_in_select(tree)
    group_by_columns = get_group_by_attributes(tree)
    repeated_columns = set(retrieve_column_name(select_columns)).intersection(
        set(retrieve_column_name(group_by_columns))
    )
    return list(repeated_columns)
def get_different_column(table, select_columns, group_by_columns):
    for col in list(tables[table].keys())[::-1]:
        if not any(col in s for s in (select_columns + group_by_columns)):
            return col
    raise KeyError("No different column found in table")


def make_select_group_by_clause_disjoint(query:str)-> tuple[str, Exception]:
    """Disjoint the select and group by clause."""
    try:
        tree = parse_one(query)
        if tree.find(exp.Group) is not None:
            for repeated_column in get_repeated_columns(tree):
                table = get_table_from_column(repeated_column)
                new_column = get_different_column(
                    table,
                    get_select_attributes(tree),
                    get_group_by_attributes(tree),
                )
                sub_sql = get_subquery(tree, repeated_column)
                query = change_select_attribute(
                    query, sub_sql, new_column, repeated_column
                )
    except Exception as e:
        return query, e
    return query, None

def get_transformation(*,is_numeric:bool):
    possibilites = [TransformationCount.COUNT, TransformationCount.DISTINCT]
    if is_numeric:
        possibilites.append(TransformationCount.MIN)
        possibilites.append(TransformationCount.MAX)
    return random.choice(possibilites)



def replace_min_max(sql):
    """
    Transform COUNT(column) in SELECT:
      - column startswith 'a' -> leave as is
      - column startswith 'b' -> COUNT(DISTINCT column)
      - column startswith 'c' -> MIN(column)
    COUNT(*) is left unchanged. Only operates on SELECT expressions.
    """
    root = parse_one(sql)

    # Only touch COUNTs in the top-level SELECT list
    select = root.find(exp.Select)
    if not select:
        return root.sql()

    def transformer(node: exp.Expression) -> exp.Expression:
        # Single-level guard clauses, no nested ifs
        if not isinstance(node, exp.Count):
            return node

        arg = node.this  # Column / Identifier / Star
        if isinstance(arg, exp.Star):
            return node  # leave COUNT(*) unchanged

        name = (
            arg.name if isinstance(arg, exp.Column)
            else (arg.this if isinstance(arg, exp.Identifier) else None)
        )
        if not name:
            return node

        table = get_table_from_column(name)
        if table is None or 'distinct' in node.sql().lower():
            return node
        is_numeric = any(keyword in tables[table][name.lower()] for keyword in ['INT', 'DECIMAL'] )
        transformation = get_transformation(is_numeric=is_numeric)
        if transformation == TransformationCount.COUNT:
            # Rewrite as COUNT(DISTINCT col)
            return node
        elif transformation == TransformationCount.DISTINCT:
            # return exp.Count(this=arg.copy(), distinct = True)
            return  exp.Count(this=exp.Distinct(expressions=[arg.copy()]))
        elif transformation == TransformationCount.MIN:
            return exp.Min(this=arg.copy())
        elif transformation == TransformationCount.MAX:
            return exp.Max(this=arg.copy())
        
        return node

    # Apply only within SELECT's expressions
    select.set(
        "expressions",
        [proj.transform(transformer) for proj in select.expressions],
    )

    return root.sql(pretty = True)



def fix_transform(params: FixTransformEndpoint) -> None:
    """Add LIMIT to sql queries according to output size."""
    random.seed(42)
    queries_folder: Path = Path(params.queries_folder)
    destination_folder = Path(params.destination_folder)
    queries_paths = list(queries_folder.glob('**/*.sql'))
    rows = []
    for query_path in tqdm(queries_paths, total=len(queries_paths)):
        query = query_path.read_text()
        # if len(list((row[DuckDBTraceEnum.duckdb_output]))) > params.max_output_size:
            # query = wrap_query_with_limit(query, params.max_output_size)

        # group by transform
        query, exception_group_by = make_select_group_by_clause_disjoint(query)

        query = replace_min_max(query)

        new_query_path = destination_folder / query_path.relative_to(queries_folder)
        new_query_path.parent.mkdir(parents=True, exist_ok=True)
        new_query_path.write_text(query)
        rows.append({
            DuckDBTraceEnum.relative_path :str( Path(query_path).relative_to(queries_folder)),
            DuckDBTraceEnum.error_group_by_sqlglot : str(exception_group_by) if exception_group_by is not None else ""
        })
    df_transformation = pl.DataFrame(rows)
    df_transformation.write_parquet(destination_folder/'transformation_log.parquet')