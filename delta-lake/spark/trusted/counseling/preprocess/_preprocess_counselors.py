from pyspark.sql.functions import col, lit, struct, array, expr
from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    concat_ws, lower, col, current_date, to_timestamp, datediff, lit, size, array_distinct
)

# Step 1: Extract all the keys in the expertise_weights struct
# expertise_fields = df.select("expertise.expertise_weights").schema[0].dataType.fields
# weight_keys = [f.name for f in expertise_fields]

# # Step 2: Build an array of structs with name + weight
# weight_structs = array(*[
#     struct(
#         lit(k).alias("name"),
#         col(f"expertise.expertise_weights.{k}").alias("weight")
#     )
#     for k in weight_keys
# ])

# # Step 3: Create the array and then filter it using SQL expression
# df_with_array = df.withColumn("expertise_weights_temp", weight_structs)

# # Use SQL-style expression to filter nulls
# df_cleaned = df_with_array.withColumn(
#     "expertise_weights_array",
#     expr("filter(expertise_weights_temp, x -> x.weight is not null)")
# )

# df_cleaned = df_cleaned.withColumn(
#     "expertise_weights_tags",
#     expr("transform(expertise_weights_array, x -> regexp_replace(x.name, '_', ' '))")
# )

# df_cleaned.first().expertise_weights_tags

# Create full name
df = df.withColumn(
    "personal_info",
    struct(
        struct(
            col("personal_info.name.first"),
            col("personal_info.name.last"),
            col("personal_info.name.title"),
            concat_ws(
                " ", col("personal_info.name.title"), col("personal_info.name.first"), col("personal_info.name.last")
            ).alias("full_name")
        ).alias("name"),
        col("personal_info.profile_image"),
        col("personal_info.contact"),
        col("personal_info.employment_history")
    )
)

df.first().personal_info.name.full_name

# Cast hire_date to date
df = df.withColumn(
    "personal_info",
    struct(
        col("personal_info.name"),
        col("personal_info.profile_image"),
        col("personal_info.contact"),
        struct(
            to_timestamp(col("personal_info.employment_history.hire_date"), "yyyy-MM-dd").alias("hire_date"),
            col("personal_info.employment_history.previous_institutions")
        ).alias("employment_history")
    )
)
df.first().personal_info.employment_history.hire_date

# Split personal_info > employment_history > previous_institutions > years into start_year and end_year
df = df.withColumn(
    "personal_info",
    struct(
        *[
            col(f"personal_info.{c}") 
            for c in df.schema["personal_info"].dataType.fieldNames() if c != "employment_history"
        ],
        struct(
            *[
                col(f"personal_info.employment_history.{c}") 
                for c in df.schema["personal_info"].dataType["employment_history"].dataType.fieldNames()
                if c != "previous_institutions"
            ],
            expr("""
                transform(
                    personal_info.employment_history.previous_institutions,
                    x -> struct(
                        x.name as name,
                        x.position as position,
                        x.years as years,
                        cast(split(x.years, '-')[0] as int) as start_year,
                        cast(split(x.years, '-')[1] as int) as end_year
                    )
                )
            """).alias("previous_institutions")
        ).alias("employment_history")
    )
)
df.first().personal_info.employment_history.previous_institutions

# Cast primary_specialization to lowercase
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "primary_specialization"
        ],
        lower(col("expertise.primary_specialization")).alias("primary_specialization")
    )
)
df.first().expertise.primary_specialization

# Cast secondary_specializations to lowercase
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "secondary_specializations"
        ],
        expr(
            "transform(expertise.secondary_specializations, x -> lower(x))"
        ).alias("secondary_specializations")
    )
)
df.first().expertise.secondary_specializations

# Remove underscores from student_populations
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "student_populations"
        ],
        expr(
            "transform(expertise.student_populations, x -> regexp_replace(x, '_', ' '))"
        ).alias("student_populations")
    )
)
df.first().expertise.student_populations

# Remove underscores from issue_expertise
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "issue_expertise"
        ],
        expr(
            "transform(expertise.issue_expertise, x -> regexp_replace(x, '_', ' '))"
        ).alias("issue_expertise")
    )
)
df.first().expertise.issue_expertise

# Remove underscores from expertise_tags
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "expertise_tags"
        ],
        expr(
            "transform(expertise.expertise_tags, x -> regexp_replace(x, '_', ' '))"
        ).alias("expertise_tags")
    )
)
df.first().expertise.expertise_tags

# expertise > expertise_weights
expertise_fields = df.select("expertise.expertise_weights").schema[0].dataType.fields
expertise_keys = [f.name for f in expertise_fields]
expertise_weight_structs = array(*[
    struct(
        lit(k).alias("expertise_weight_name"),
        col(f"expertise.expertise_weights.{k}").alias("value")
    )
    for k in expertise_keys
])
# Inject column
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "expertise_weights"
        ],
        expertise_weight_structs.alias("expertise_weights_count")
    )
)
# Filter column to keep only applicable fields
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "expertise_weights_count"
        ],
        expr("""
            filter(
                expertise.expertise_weights_count,
                x -> x.value is not null
            )
        """).alias("expertise_weights_count")
    )
)
# Replace _ and create column of intervention types
df = df.withColumn(
    "expertise",
    struct(
        *[
            col(f"expertise.{c}") 
            for c in df.schema["expertise"].dataType.fieldNames() if c != "expertise_weights_count"
        ],
        expr("""
            transform(
                expertise.expertise_weights_count,
                x -> struct(
                    regexp_replace(x.expertise_weight_name, '_', ' ') as expertise_weight_name,
                    x.value as value
                )
            )
        """).alias("expertise_weights_count"),
        expr("""
            transform(
                expertise.expertise_weights_count,
                x -> regexp_replace(x.expertise_weight_name, '_', ' ')
            )
        """).alias("expertise_weights_types")
    )
)
df.first().expertise.expertise_weights_count
df.first().expertise.expertise_weights_types

# Cast dates in qualifications > certifications to dates
df = df.withColumn(
    "qualifications",
    struct(
        *[
            col(f"qualifications.{c}") 
            for c in df.schema["qualifications"].dataType.fieldNames() if c != "certifications"
        ],
        expr("""
            transform(
                qualifications.certifications,
                x -> struct(
                    lower(x.name) as name,
                    lower(x.organization) as organization,
                    to_timestamp(x.date_obtained) as date_obtained,
                    to_timestamp(x.expiration_date) as expiration_date
                )
            )
        """).alias("certifications")
    )
)
df.schema["qualifications"].dataType["certifications"].dataType
df.first().qualifications.certifications

# Cast languages to lowercase
df = df.withColumn(
    "qualifications",
    struct(
        *[
            col(f"qualifications.{c}") 
            for c in df.schema["qualifications"].dataType.fieldNames() if c != "languages"
        ],
        expr("""
            transform(
                qualifications.languages,
                x -> struct(
                    lower(x.language) as language,
                    lower(x.proficiency) as proficiency
                )
            )
        """).alias("languages")
    )
)
df.first().qualifications.languages

# Cast professional_memberships to lowercase
df = df.withColumn(
    "qualifications",
    struct(
        *[
            col(f"qualifications.{c}") 
            for c in df.schema["qualifications"].dataType.fieldNames() if c != "professional_memberships"
        ],
        expr("transform(qualifications.professional_memberships, x -> lower(x))") \
        .alias("professional_memberships")
    )
)
df.first().qualifications.professional_memberships

# performance_metrics > intervention_statistics
# to process: by_intervention_type & success_metrics
# by_intervention_type
intervention_fields = df.select("performance_metrics.intervention_statistics.by_intervention_type").schema[0].dataType.fields
intervention_keys = [f.name for f in intervention_fields]
intervention_count_structs = array(*[
    struct(
        lit(k).alias("intervention_type"),
        col(f"performance_metrics.intervention_statistics.by_intervention_type.{k}").alias("count")
    )
    for k in intervention_keys
])
# Inject column
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "intervention_statistics"
        ],
        struct(
            *[
                col(f"performance_metrics.intervention_statistics.{c}") 
                for c in df.schema["performance_metrics"].dataType["intervention_statistics"] \
                .dataType.fieldNames() if c != "by_intervention_type"
            ],
            intervention_count_structs.alias("by_intervention_type_count")
        ).alias("intervention_statistics")
    )
)
# Filter column to keep only applicable fields
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "intervention_statistics"
        ],
        struct(
            *[
                col(f"performance_metrics.intervention_statistics.{c}") 
                for c in df.schema["performance_metrics"].dataType["intervention_statistics"] \
                .dataType.fieldNames() if c != "by_intervention_type_count"
            ],
            expr("""
                filter(
                    performance_metrics.intervention_statistics.by_intervention_type_count,
                    x -> x.count is not null
                )
            """).alias("by_intervention_type_count")
        ).alias("intervention_statistics")
    )
)
# Replace _ and create column of intervention types
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "intervention_statistics"
        ],
        struct(
            *[
                col(f"performance_metrics.intervention_statistics.{c}") 
                for c in df.schema["performance_metrics"].dataType["intervention_statistics"] \
                .dataType.fieldNames() if c != "by_intervention_type_count"
            ],
            expr("""
                transform(
                    performance_metrics.intervention_statistics.by_intervention_type_count,
                    x -> struct(
                        regexp_replace(x.intervention_type, '_', ' ') as intervention_type,
                        x.count as count
                    )
                )
            """).alias("by_intervention_type_count"),
            expr("""
                transform(
                    performance_metrics.intervention_statistics.by_intervention_type_count,
                    x -> regexp_replace(x.intervention_type, '_', ' ')
                )
            """).alias("intervention_types")
        ).alias("intervention_statistics")
    )
)
df.first().performance_metrics.intervention_statistics.by_intervention_type_count
df.first().performance_metrics.intervention_statistics.intervention_types

# success_metrics
metric_fields = df.select("performance_metrics.intervention_statistics.success_metrics").schema[0].dataType.fields
metric_keys = [f.name for f in metric_fields]
metric_count_structs = array(*[
    struct(
        lit(k).alias("metric_name"),
        col(f"performance_metrics.intervention_statistics.success_metrics.{k}").alias("value")
    )
    for k in metric_keys
])
# Inject column
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "intervention_statistics"
        ],
        struct(
            *[
                col(f"performance_metrics.intervention_statistics.{c}") 
                for c in df.schema["performance_metrics"].dataType["intervention_statistics"] \
                .dataType.fieldNames() if c != "success_metrics"
            ],
            metric_count_structs.alias("success_metrics_count")
        ).alias("intervention_statistics")
    )
)
# Filter column to keep only applicable fields
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "intervention_statistics"
        ],
        struct(
            *[
                col(f"performance_metrics.intervention_statistics.{c}") 
                for c in df.schema["performance_metrics"].dataType["intervention_statistics"] \
                .dataType.fieldNames() if c != "success_metrics_count"
            ],
            expr("""
                filter(
                    performance_metrics.intervention_statistics.success_metrics_count,
                    x -> x.value is not null
                )
            """).alias("success_metrics_count")
        ).alias("intervention_statistics")
    )
)
# Replace _ and create column of intervention types
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "intervention_statistics"
        ],
        struct(
            *[
                col(f"performance_metrics.intervention_statistics.{c}") 
                for c in df.schema["performance_metrics"].dataType["intervention_statistics"] \
                .dataType.fieldNames() if c != "success_metrics_count"
            ],
            expr("""
                transform(
                    performance_metrics.intervention_statistics.success_metrics_count,
                    x -> struct(
                        regexp_replace(x.metric_name, '_', ' ') as metric_name,
                        x.value as value
                    )
                )
            """).alias("success_metrics_count"),
            expr("""
                transform(
                    performance_metrics.intervention_statistics.success_metrics_count,
                    x -> regexp_replace(x.metric_name, '_', ' ')
                )
            """).alias("success_metric_types")
        ).alias("intervention_statistics")
    )
)
df.first().performance_metrics.intervention_statistics.success_metrics_count
df.first().performance_metrics.intervention_statistics.success_metric_types

# performance_metrics > student_demographics > by_major
major_fields = df.select("performance_metrics.student_demographics.by_major").schema[0].dataType.fields
major_keys = [f.name for f in major_fields]
major_count_structs = array(*[
    struct(
        lit(k).alias("major_code"),
        col(f"performance_metrics.student_demographics.by_major.{k}").alias("count")
    )
    for k in major_keys
])
# Inject column
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "student_demographics"
        ],
        struct(
            *[
                col(f"performance_metrics.student_demographics.{c}") 
                for c in df.schema["performance_metrics"].dataType["student_demographics"] \
                .dataType.fieldNames() if c != "by_major"
            ],
            major_count_structs.alias("by_major_count")
        ).alias("student_demographics")
    )
)
# Filter column to keep only applicable fields
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "student_demographics"
        ],
        struct(
            *[
                col(f"performance_metrics.student_demographics.{c}") 
                for c in df.schema["performance_metrics"].dataType["student_demographics"] \
                .dataType.fieldNames() if c != "by_major_count"
            ],
            expr("""
                filter(
                    performance_metrics.student_demographics.by_major_count,
                    x -> x.count is not null
                )
            """).alias("by_major_count")
        ).alias("student_demographics")
    )
)
df.first().performance_metrics.student_demographics.by_major_count
# Replace _
df = df.withColumn(
    "performance_metrics",
    struct(
        *[
            col(f"performance_metrics.{c}") 
            for c in df.schema["performance_metrics"].dataType.fieldNames() if c != "student_demographics"
        ],
        struct(
            *[
                col(f"performance_metrics.student_demographics.{c}") 
                for c in df.schema["performance_metrics"].dataType["student_demographics"] \
                .dataType.fieldNames() if c != "by_major_count"
            ],
            expr("""
                transform(
                    performance_metrics.student_demographics.by_major_count,
                    x -> struct(
                        regexp_replace(x.major_code, '_', ' ') as metric_name,
                        x.count as count
                    )
                )
            """).alias("by_major_count")
        ).alias("student_demographics")
    )
)

# availability > weekly_schedule
df = df.withColumn(
    "availability",
    struct(
        *[
            col(f"availability.{c}") 
            for c in df.schema["availability"].dataType.fieldNames() if c != "weekly_schedule"
        ],
        expr("""
            flatten(array(
                transform(availability.weekly_schedule.monday, x -> struct('monday' as weekday, x.start as start_time, x.end as end_time)),
                transform(availability.weekly_schedule.tuesday, x -> struct('tuesday' as weekday, x.start as start_time, x.end as end_time)),
                transform(availability.weekly_schedule.wednesday, x -> struct('wednesday' as weekday, x.start as start_time, x.end as end_time)),
                transform(availability.weekly_schedule.thursday, x -> struct('thursday' as weekday, x.start as start_time, x.end as end_time)),
                transform(availability.weekly_schedule.friday, x -> struct('friday' as weekday, x.start as start_time, x.end as end_time))
            ))
        """).alias("weekly_schedule")
    )
)
df.first().availability.weekly_schedule

# availability > upcoming_unavailability: cast start_date, end_date to date
df = df.withColumn(
    "availability",
    struct(
        *[
            col(f"availability.{c}") 
            for c in df.schema["availability"].dataType.fieldNames() if c != "upcoming_unavailability"
        ],
        expr("""
            transform(
                availability.upcoming_unavailability,
                x -> struct(
                    to_timestamp(x.start_date) as start_date,
                    to_timestamp(x.end_date) as end_date,
                    lower(x.reason) as reason
                )
            )
        """).alias("upcoming_unavailability")
    )
)
df.first().availability.upcoming_unavailability

landing_dir = 'file:///data/landing/counselor_api/date=2025-05-25/batch=d82ac817-44e1-42de-bd5a-e6fae9d5eeaf'
df = spark.read.format('delta').load(landing_dir)

from datetime import datetime
process_timestamp = datetime.now()
process_date = process_timestamp.strftime("%Y-%m-%d")
process_time = process_timestamp.strftime("%H-%M-%S")
df = df.withColumn("_process_timestamp", lit(process_timestamp.isoformat())) \
        .withColumn("_process_date", lit(process_date))

import json
clean_counselors = [json.loads(row) for row in df.toJSON().collect()]
counselors_collection.insert_many(clean_counselors)

counselors_collection.delete_many({})