# Explanation

Two filtering methods are available, though they are not mutually exclusive:
1. Emtpy set: Removes queries with `0` in the `count_star` column.
2. Stratified sampling: Divides queries into equi-width bins based on the 
    `count_star` values and samples up to a specified number of queries 
    from each bin.


# Attributes

- empty_set (bool): Whether to filter out empty set from the results.
- stratified_sampling (bool): Whether to cherry-pick queries based on specific
    criteria.
- stratified_sampling_config: Configuration for cherry-picking
    queries. This is required if `stratified_sampling` is set to True.

## Stratified sampling 

- queries_per_bin (int): total queries to sample from each bin.
- upper_bound (int): The upper bound for the `count_star` values to
    consider when creating bins. Any queries with `count_star` values
    above this threshold will be grouped into the last bin.
- total_bins (int): The total number of equi-width bins to create
    between 0 and the `upper_bound`.


# Output

Additionally to filtering this filter will group the queries by their
join signature. Meaning that queries that have the exact same join structure
will be under the same folder. The name of the folder will be the integer
representation of the bitmap that identifies the join signature.

The name of the queries will change from 
`batch_{#batch}/{fact_table}_{#id_per_fact_table}_{#id_predicates}.sql`
to
`{join_signature}/{fact_table}_{#id_per_fact_table}_{#id_predicates}_{#batch}.sql`

The output with the `bin` information for stratified sampling is located
in the `filtered.parquet`.
