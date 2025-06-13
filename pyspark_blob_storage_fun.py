import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, when, month, year, lit, lpad, concat

# Gets rid of borders and bolding in headers in xlsx files
from pandas.io.formats import excel
# Removes bold and borders in excel header.
excel.ExcelFormatter.header_style = None

# Fill in the schema with the columns you need from the exercise instructions
pet_act_schema = StructType([StructField("pet_id", IntegerType()),
                             StructField("date", DateType()),
                             StructField("activity_type", StringType()),
                             StructField("duration_minutes", StringType()),
                             ])

pet_health_schema = StructType([StructField("pet_id", IntegerType()),
                                StructField("visit_date", DateType()),
                                StructField("issue", StringType()),
                                StructField("resolution", StringType()),
                                ])

user_schema = StructType([StructField("owner_id", IntegerType()),
                          StructField("pet_id", IntegerType()),
                          StructField("owner_age_group", StringType()),
                          StructField("pet_type", StringType()),
                          ])

spark = SparkSession.builder \
    .appName("AzureBlobRead") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.1,"
            "com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()


def ensure_pet_activities(pet_activities_df):
    null_count = pet_activities_df.filter(col("date").isNull()).count()
    assert null_count == 0


def ensure_pet_health(pet_health_df):
    null_count = pet_health_df.filter(col("pet_id").isNull()).count()
    assert null_count == 0


def ensure_users(users_df):
    null_count = users_df.filter(col("pet_id").isNull()).count()
    assert null_count == 0


def merge_and_transform_dfs(pet_activities1, pet_health1, users1):
    # Ensure activate data has a same 'date' name, so they can concat on each other
    pet_health1 = pet_health1.withColumnRenamed('visit_date', 'date')

    # Concat heath and activities table
    pet_health_and_act = pet_health1.unionByName(pet_activities1, allowMissingColumns=True)

    # Rows that relate to a health visit, the value 'Health'
    pet_health_and_act = pet_health_and_act.withColumn(
        'activity_type',
        when(col('activity_type').isNull(), 'Health').otherwise(col('activity_type'))
    )

    # Only 'Walking', 'Playing' or 'Resting'
    pet_health_and_act = pet_health_and_act.withColumn(
        'activity_type',
        when(col('activity_type') == 'Walk', 'Walking')
        .when(col('activity_type') == 'Play', 'Playing')
        .when(col('activity_type') == 'Rest', 'Resting')
        .when(col('activity_type') == '', '')
        .otherwise(col('activity_type'))
    )

    # Replace '-' with null in 'duration_minutes'
    pet_health_and_act = pet_health_and_act.withColumn(
        'duration_minutes',
        when(col('duration_minutes') == '-', None).otherwise(col('duration_minutes'))
    )

    pet_health_and_act.printSchema()

    pet_health_and_act = pet_health_and_act.withColumn(
        'duration_minutes',
        when(col('activity_type') == 'Health', '0').otherwise(col('duration_minutes'))
    )

    pet_health_and_act.printSchema()
    # pet_health_and_act = pet_health_and_act.withColumn("duration_minutes", col("duration_minutes").try_cast("double"))

    # Fill nulls in 'issue' and 'resolution' with None (equivalent to np.nan in PySpark)
    pet_health_and_act = pet_health_and_act.na.fill({'issue': '', 'resolution': ''})

    # Perform a left join on 'pet_id'
    pet_info_and_users = users1.join(pet_health_and_act, on='pet_id', how='left')

    # Add Month Column For Analysis 2024-10-20 -> 2024-10. Didn't want to just right strip 7 characters.
    pet_info_and_users = pet_info_and_users.withColumn("month", month("date"))
    pet_info_and_users = pet_info_and_users.withColumn("month", lpad("month", 2, "0"))
    pet_info_and_users = pet_info_and_users.withColumn("year", year("date"))
    pet_info_and_users = pet_info_and_users.withColumn(
        "year_month", concat(pet_info_and_users["year"], lit("-"), pet_info_and_users["month"])
    )

    # Rename 'final_date' to 'date'. Added year_month
    pet_info_and_users = pet_info_and_users.withColumnRenamed('final_date', 'date')
    pet_info_and_users = pet_info_and_users.withColumn("activity_type", col("activity_type").cast("string"))
    pet_info_and_users = pet_info_and_users.withColumn("owner_age_group", col("owner_age_group").cast("string"))
    pet_info_and_users = pet_info_and_users.withColumn("pet_type", col("pet_type").cast("string"))
    pet_info_and_users = pet_info_and_users.select('pet_id', 'date', 'activity_type', 'duration_minutes', 'issue',
                                                   'resolution', 'owner_id',
                                                   'owner_age_group', 'pet_type', 'year_month')

    return pet_info_and_users


def all_pet_data(pet_activities_filename, pet_health_filename, users_filename):
    # Read
    pet_activities = spark.read.csv(
        f"wasbs://dataforpethealth@petdatavcresc.blob.core.windows.net/{pet_activities_filename}",
        header=True,
        schema=pet_act_schema,
        enforceSchema=True
    )

    pet_health = spark.read.csv(
        f"wasbs://dataforpethealth@petdatavcresc.blob.core.windows.net/{pet_health_filename}",
        header=True,
        schema=pet_health_schema,
        enforceSchema=True
    )

    users = spark.read.csv(
        f"wasbs://dataforpethealth@petdatavcresc.blob.core.windows.net/{users_filename}",
        header=True,
        schema=user_schema,
        enforceSchema=True
    )

    # Ensure
    ensure_pet_activities(pet_activities)
    ensure_pet_health(pet_health)
    ensure_users(users)

    # Merge
    final_df = merge_dfs(pet_activities, pet_health, users)


    return final_df


def create_excel_report(pet_data):
    number_of_owners = pet_data.select("owner_id").distinct().count()
    number_of_pets = pet_data.select("pet_id").distinct().count()

    # Pie Chart (Count of Different Pets Entered on the App)
    pet_counts = pet_data.select("pet_id", "pet_type").distinct()
    grouped_counts_pie = pet_counts.groupBy("pet_type").count()
    grouped_counts_pie_pd = grouped_counts_pie.toPandas()
    pie_max_row = grouped_counts_pie_pd.shape[0]

    # Radar Chart (Count of Different Age Groups Using the App)
    age_group_counts = pet_data.select("owner_id", "owner_age_group").distinct()
    age_group_counts_radar = age_group_counts.groupBy("owner_age_group").count()
    age_group_counts_radar_pd = age_group_counts_radar.toPandas()
    radar_max_row = age_group_counts_radar_pd.shape[0]

    # Line Chart (Usage (people entering an event in the app) over time)
    year_month_usage_counts = pet_data.select("year_month")
    year_month_counts = year_month_usage_counts.groupBy("year_month").count()
    year_month_counts = year_month_counts.sort("year_month", ascending=True)
    year_month_counts_pd = year_month_counts.toPandas()
    year_month_max_row = year_month_counts_pd.shape[0]

    # Bar Chart (What users log the most of)
    activity_type_counts = pet_data.select("activity_type")
    activity_type_counts = activity_type_counts.groupBy("activity_type").count()
    activity_type_counts_pd = activity_type_counts.toPandas()
    activity_type_max_row = activity_type_counts_pd.shape[0]

    # Change to pandas for Excel writer and sending raw data to 'Raw Data' tab
    pet_data = pet_data.toPandas()

    with pd.ExcelWriter(f"./final_output/Pet_Data_Report.xlsx", engine='xlsxwriter') as writer:
        workbook = writer.book
        # Font Format For Main Stats
        bold_large_format = workbook.add_format({
            'bold': True,
            'font_size': 14
        })

        empty_df = pd.DataFrame()
        empty_df.to_excel(writer, sheet_name=f'Summary', index=False, startrow=0, startcol=0, header=False)
        pet_data.to_excel(writer, sheet_name=f'Raw Data', index=False, startrow=0, startcol=0, header=True)
        empty_df.to_excel(writer, sheet_name=f'Data Tables For Summary', index=False, startrow=0, startcol=0, header=False)
        summary_worksheet = writer.sheets['Summary']
        summary_worksheet.set_column('A:A', 42)
        summary_worksheet.write(1, 0, "Number of Owners/Users: ", bold_large_format)
        summary_worksheet.write(1, 1, number_of_owners)
        summary_worksheet.write(2, 0, "Number of Pets: ", bold_large_format)
        summary_worksheet.write(2, 1, number_of_pets)

        # Data Placed in Tab to reference to for chart creations.
        grouped_counts_pie_pd.to_excel(writer, sheet_name=f'Data Tables For Summary', index=False, startrow=1,
                                       startcol=1, header=True)
        age_group_counts_radar_pd.to_excel(writer, sheet_name=f'Data Tables For Summary', index=False, startrow=1,
                                           startcol=4, header=True)
        year_month_counts_pd.to_excel(writer, sheet_name=f'Data Tables For Summary', index=False, startrow=1,
                                      startcol=7, header=True)
        activity_type_counts_pd.to_excel(writer, sheet_name=f'Data Tables For Summary', index=False, startrow=1,
                                         startcol=10, header=True)

        # ---------------- Pie (Count of Different Pets Entered on the App)
        pie_chart = workbook.add_chart({'type': 'pie'})
        pie_chart.set_title({
            'name': 'Pet Type Breakdown',
        })
        pie_chart.set_size({'width': 850, 'height': 550})
        #  [sheetname, first_row, first_col, last_row, last_col]
        pie_chart.add_series({'categories': ['Data Tables For Summary', 2, 1, pie_max_row + 1, 1],
                              "name": "Pet Type Breakdown",
                              'values': ['Data Tables For Summary', 2, 2, pie_max_row + 1, 2],
                              'data_labels': {'percentage': True, 'leader_lines': True, "position": "inside_end",
                                              'font': {'name': 'Consolas', 'color': 'black'}},
                              })
        pie_chart.set_style(40)
        summary_worksheet.insert_chart('A6', pie_chart, {'x_scale': 0.5, 'y_scale': 0.5})

        # ---------------- Age Group Chart (Radar Chart)
        radar_chart = workbook.add_chart({'type': 'radar', 'subtype': 'filled'})
        radar_chart.set_title({
            'name': 'Age Group Distribution',
        })
        radar_chart.set_size({'width': 850, 'height': 550})
        #  [sheetname, first_row, first_col, last_row, last_col]
        radar_chart.add_series({'categories': ['Data Tables For Summary', 2, 4, radar_max_row + 1, 4],
                                "name": "Age Group Distribution",
                                'values': ['Data Tables For Summary', 2, 5, radar_max_row + 1, 5],
                                })
        radar_chart.set_style(40)
        summary_worksheet.insert_chart('G6', radar_chart, {'x_scale': 0.5, 'y_scale': 0.5})

        # ---------------- Line Chart (Usage (people entering an event in the app) over time
        line_chart = workbook.add_chart({'type': 'line'})
        line_chart.set_title({
            'name': 'Data Entries (App Usage) Over Time',
        })
        line_chart.set_legend({'none': True})
        line_chart.set_size({'width': 850, 'height': 550})
        #  [sheetname, first_row, first_col, last_row, last_col]
        line_chart.add_series({'categories': ['Data Tables For Summary', 2, 7, year_month_max_row + 1, 7],
                               "name": 'Data Entries (App Usage) Over Time',
                               'values': ['Data Tables For Summary', 2, 8, year_month_max_row + 1, 8],
                               'data_labels': {'value': True},
                               'font': {'rotation': 45},
                               })
        line_chart.set_style(40)
        summary_worksheet.insert_chart('A23', line_chart, {'x_scale': 0.8, 'y_scale': 0.6})

        # ---------------- Bar Chart (What users log the most of)
        bar_chart = workbook.add_chart({'type': 'column'})
        bar_chart.set_title({
            'name': 'Activity Type Frequency',
        })
        bar_chart.set_legend({'none': True})
        bar_chart.set_size({'width': 850, 'height': 550})
        #  [sheetname, first_row, first_col, last_row, last_col]
        bar_chart.add_series({'categories': ['Data Tables For Summary', 2, 10, activity_type_max_row + 1, 10],
                              "name": 'Activity Type Frequency',
                              'values': ['Data Tables For Summary', 2, 11, activity_type_max_row + 1, 11],
                              'data_labels': {'value': True},
                              'font': {'rotation': 45},
                              })
        bar_chart.set_style(40)
        summary_worksheet.insert_chart('J23', bar_chart, {'x_scale': 0.5, 'y_scale': 0.5})
        summary_worksheet.active()


if __name__ == '__main__':
    start = time.time()
    spark.conf.set(
        "fs.azure.account.key.petdatavcresc.blob.core.windows.net",
        "Secret Key"
    )

    consolidated_pet_data = all_pet_data('pet_activities.csv', 'pet_health.csv', 'users.csv')

    # Repartitioning to simulate distributed writing and demonstrate Spark parallelism on output.
    consolidated_pet_data.repartition(2).write.csv("final_output", header=True, mode="overwrite", sep=',')

    create_excel_report(consolidated_pet_data)

    end = time.time()
    print(f'{end - start: .4f}')




