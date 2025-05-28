from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, struct, array, expr, concat_ws, lower, to_timestamp

def create_full_name(df):
    """
    Create a full_name field under personal_info.name
    """
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
    return df

def cast_hire_date(df):
    """
    Cast personal_info.employment_history.hire_date to date type
    """
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
    return df

def split_employment_years(df):
    """
    Split personal_info.employment_history.previous_institutions.years
    into start_year and end_year
    """
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
    return df

def lower_primary_spec(df):
    """
    Cast primary_specialization to lowercase
    """
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
    return df

def lower_secondary_spec(df):
    """
    Cast secondary_specializations to lowercase
    """
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
    return df

def remove_underscore_student_populations(df):
    """
    Remove underscores from student_populations
    """
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
    return df

def remove_underscore_issue_expertise(df):
    """
    Remove underscores from issue_expertise
    """
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
    return df

def remove_underscore_expertise_tags(df):
    """
    Remove underscores from expertise_tags
    """
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
    return df

def replace_expertise_weights(df):
    """
    Fix schema of expertise.expertise_weights
    Create array of tags
    """
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
    return df

def cast_certification_date(df):
    """
    Cast dates in qualifications.certifications to dates
    """
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
    return df

def lower_languages(df):
    """
    Cast languages to lowercase
    """
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
    return df

def lower_prof_memberships(df):
    """
    Cast qualifications.professional_memberships to lowercase
    """
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
    return df

def replace_intervention_type(df):
    """
    Fix schema of performance_metrics.intervention_statistics.by_intervention_type
    """
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
    return df

def replace_success_metrics(df):
    """
    Fix schema of performance_metrics.intervention_statistics.success_metrics
    """
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
    return df

def replace_majors(df):
    """
    Fix schema of performance_metrics.student_demographics.by_major
    """
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
                            regexp_replace(x.major_code, '_', ' ') as major_code,
                            x.count as count
                        )
                    )
                """).alias("by_major_count")
            ).alias("student_demographics")
        )
    )
    return df

def replace_weekly_schedule(df):
    """
    Fix schema of availability.weekly_schedule
    """
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
    return df

def cast_unavailability(df):
    """
    Cast availability.upcoming_unavailability.start_date/end_date to date type
    """
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
    return df

def preprocessing_pipeline(df):
    df = create_full_name(df)
    df = cast_hire_date(df)
    df = split_employment_years(df)
    df = lower_primary_spec(df)
    df = lower_secondary_spec(df)
    df = remove_underscore_student_populations(df)
    df = remove_underscore_issue_expertise(df)
    df = remove_underscore_expertise_tags(df)
    df = replace_expertise_weights(df)
    df = cast_certification_date(df)
    df = lower_languages(df)
    df = lower_prof_memberships(df)
    df = replace_intervention_type(df)
    df = replace_success_metrics(df)
    df = replace_majors(df)
    df = replace_weekly_schedule(df)
    df = cast_unavailability(df)
    return df