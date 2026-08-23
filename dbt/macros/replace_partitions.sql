{% macro replace_partitions() %}
{% if is_incremental() %}
    delete from {{ this }} where source_period in (
    {% for period in var('changed_periods', []) %}'{{ period }}'{% if not loop.last %},{% endif %}{% else %}null{% endfor %}
    )
{% else %}
    select 1
{% endif %}
{% endmacro %}
