{% test duplicate_count(model, columns_to_check, group_by, where_condition, warn_threshold, error_threshold) %}

    with duplicate_counts as (
        select
            {% if group_by %}
                {{ group_by | join(", ") }},
            {% endif %}
            {% for column in columns_to_check %}
                '{{ column }}' as ColumnName,
                count(*) as DuplicateCount
            from {{ model }}
            where {{ column }} is not null
            {% if where_condition %} and {{ where_condition }} {% endif %}
            {% if group_by %}group by {{ column }}, {{ group_by | join(", ") }}{% else %}group by {{ column }}{% endif %}
            having count(*) > 1
                and (count(*) >= {{ error_threshold }}
                or count(*) >= {{ warn_threshold }})
            {% if not loop.last %}union all select{% endif %}
            {% endfor %}
    )
    select
        {{ group_by | join(", ") }},
        ColumnName,
        sum(DuplicateCount) as DuplicateCount,
        {{ warn_threshold }} AS WarningThreshold,
        {{ error_threshold }} AS ErrorThreshold,
        CASE 
          WHEN (sum(DuplicateCount)) >= {{ warn_threshold }} THEN 'Warning'
          WHEN (sum(DuplicateCount)) >= {{ error_threshold }} THEN 'Fail'
          ELSE 'Pass'
        END AS Outcome

    from duplicate_counts
    group by {{ group_by | join(", ") }}, ColumnName
{% endtest %}
