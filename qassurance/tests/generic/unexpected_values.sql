{% test unexpected_values(model, columns_to_check, group_by, where_condition) %}
    with unexpected_values as (
        select
            {{ model }}.*,
            {% for column, acceptable_values in columns_to_check.items() %}
                case when {{ column }} not in ('{{ acceptable_values | join("', '") }}') then '{{ column }}: ' || {{ column }} end as UnexpectedValue{{ loop.index }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ model }}
        {% if where_condition %}where {{ where_condition }}{% endif %}
    )
    select
        *
    from unexpected_values
    where
        {% for i in range(1, columns_to_check|length + 1) %}
            UnexpectedValue{{ i }} is not null{% if not loop.last %} or {% endif %}
        {% endfor %}
{% endtest %}
