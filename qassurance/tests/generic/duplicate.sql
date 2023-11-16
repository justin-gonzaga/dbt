{% test duplicate(model, columns_to_check, group_by, where_condition) %}
    with duplicate_values as (
        select
            {{ model }}.*,
            {% for column in columns_to_check %}
                case when count(*) over (partition by {{ column }}) > 1 then '{{ column }}: ' || {{ column }} end as DuplicateValue{{ loop.index }}
                {% if not loop.last %}union all{% endif %}
            {% endfor %}
        from {{ model }}
        {% if where_condition %}where {{ where_condition }}{% endif %}
    )
    select
        *
    from duplicate_values
    where
        {% for i in range(1, columns_to_check|length + 1) %}
            DuplicateValue{{ i }} is not null{% if not loop.last %} or {% endif %}
        {% endfor %}
{% endtest %}
