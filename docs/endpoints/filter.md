

# Attributes
Two filtering methods are available, though they are not mutually exclusive:
1. Null Filter: Removes queries with null values in the `count_star`
    column.
2. Cherry-Pick Filter: Divides queries into equi-width bins based on the 
    `count_star` values and samples up to a specified number of queries 
    from each bin.
# Attributes
- filter_null (bool): Whether to filter out null values from the results.
- cherry_pick (bool): Whether to cherry-pick queries based on specific
    criteria.
- cherry_pick_config (CherryPickBase): Configuration for cherry-picking
    queries. This is required if `cherry_pick` is set to True.
- queries_per_bin (int): total queries to sample from each bin.
- upper_bound (int): The upper bound for the `count_star` values to
    consider when creating bins. Any queries with `count_star` values
    above this threshold will be grouped into the last bin.
- total_bins (int): The total number of equi-width bins to create
    between 0 and the `upper_bound`.

Examples of toml files can be found in:
`params_config/filter/*toml`