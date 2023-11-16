{% test nullability(model, columns_to_check, group_by, where_condition, lower_threshold=-1, upper_threshold=1) %}
    with null_counts as (
        select
            {% for column in columns_to_check %}
                '{{ column }}' as ColumnName,
                count(*) as NullCount
            from {{ model }}
            where {{ column }} is null
            {% if where_condition %} and {{ where_condition }} {% endif %}
            group by {{ column }}
            having count(*) <= {{ lower_threshold }} OR count(*) >= {{ upper_threshold }}
            {% if not loop.last %}union all select{% endif %}
            {% endfor %}
    )
    select
        ColumnName,
        sum(NullCount) as NullCount
    from null_counts
    group by ColumnName
{% endtest %}
