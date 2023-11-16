{% test comparison(model, compare_model, compare_columns=None, lower_threshold=-1, upper_threshold=1, metric=[], group_by=[]) %}

{#-
If the compare_cols arg is provided, we can run this test without querying the
information schema — this allows the model to be an ephemeral model
-#}
{%- if not compare_columns -%}
    {%- if execute -%}
        {%- do dbt_utils._is_ephemeral(model, 'calc_comp') -%}
        {%- set compare_columns = adapter.get_columns_in_relation(model) -%}
    {% endif -%}
{%- endif -%}

{% set compare_cols_csv = compare_columns | map(attribute='name') | join(', ') %}

with a as (
    select * from {{ model }}
),
b as (
    select * from {{ compare_model }}
),
final as (
    select
        {% if group_by %}
            {% for col in group_by %}
                COALESCE(a.{{ col }}, b.{{ col }}),
            {% endfor %}
        {% endif %}
        {{ lower_threshold }} as WarningThreshold,
        {{ upper_threshold }} as ErrorThreshold,
        {% if metric %}
            {% for col in metric %}
                COALESCE(SUM(a.{{ col }}), 0) - COALESCE(SUM(b.{{ col }}), 0) AS Diff_{{ col }}{% if not loop.last %},{% endif %}
            {% endfor %}
        {% else %}
            COALESCE(COUNT(a.*), 0) - COALESCE(COUNT(b.*), 0) AS RecordDiff
        {% endif %},
        {% if metric %}
            {% for col in metric %}
                CASE 
                    WHEN COALESCE(SUM(a.{{ col }}), 0) - COALESCE(SUM(b.{{ col }}), 0) < {{ lower_threshold }} THEN 'Fail'
                    WHEN COALESCE(SUM(a.{{ col }}), 0) - COALESCE(SUM(b.{{ col }}), 0) = {{ lower_threshold }} THEN 'Warning'
                    WHEN COALESCE(SUM(a.{{ col }}), 0) - COALESCE(SUM(b.{{ col }}), 0) > {{ upper_threshold }} THEN 'Fail'
                    WHEN COALESCE(SUM(a.{{ col }}), 0) - COALESCE(SUM(b.{{ col }}), 0) = {{ upper_threshold }} THEN 'Warning'
                    ELSE 'Pass'
                END AS Outcome_{{ col }}{% if not loop.last %},{% endif %}
            {% endfor %}
        {% else %}
            CASE 
                WHEN COALESCE(COUNT(a.*), 0) - COALESCE(COUNT(b.*), 0) < {{ lower_threshold }} THEN 'Fail'
                WHEN COALESCE(COUNT(a.*), 0) - COALESCE(COUNT(b.*), 0) = {{ lower_threshold }} THEN 'Warning'
                WHEN COALESCE(COUNT(a.*), 0) - COALESCE(COUNT(b.*), 0) > {{ upper_threshold }} THEN 'Fail'
                WHEN COALESCE(COUNT(a.*), 0) - COALESCE(COUNT(b.*), 0) = {{ upper_threshold }} THEN 'Warning'
                ELSE 'Pass'
            END AS Outcome
        {% endif %}
    FROM
        a
    FULL JOIN
        b ON a.TxnStartDate = b.TxnStartDate
    GROUP BY 
        {% if group_by %}
            {% for col in group_by %}
                COALESCE(a.{{ col }}, b.{{ col }}){% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
)

select *
from final

{% endtest %}
