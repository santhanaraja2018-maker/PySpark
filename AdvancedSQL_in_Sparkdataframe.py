from pyspark.sql import SparkSession

#create seesion
spark = SparkSession.builder.appName("AdvancedSQLin SPARK DATA FRAMES").master("local[*]").getOrCreate()

#subquries
employee_data = [
    (1,"John"),(2,"Raja"),(3,"Nive"),(4,"Jana"),(5,"Muthuvel"),(6,"Sumathi"),(7,"Santhanam"),(8,"Valli")
]

employees = spark.createDataFrame(employee_data,["id","name"])

salary_data = [
    ("HR",1,5000),("HR",2,6000),("HR",3,9000),
    ("IT",4,8000),("IT",5,6600),
    ("SALES",6,5500),("SALES",7,6300),("SALES",8,6300)
]

salaries = spark.createDataFrame(salary_data,["department","id","salary"])

employees.show()
salaries.show()

#register as temp views
employees.createOrReplaceTempView("employees")
salaries.createOrReplaceTempView("salaries")

#subquery to find employees with above average salaries
result = spark.sql (
    """select * from employees where id in (
    select id from salaries where salary > (select avg(salary) from salaries))"""
)
print("\nThe employees whose salaries are greater than the average salary")
result.show()


#window functions
from pyspark.sql.window import Window
from pyspark.sql import functions as F

employee_salary = spark.sql(""" select salaries.*,employees.name from salaries 
                            left join employees on salaries.id = employees.id""")

print("\nJoined Table")
employee_salary.show()


#creating window spec
window_spec = Window.partitionBy("department").orderBy(F.desc("Salary"))

#Claculate rank of employee with each department
rank = employee_salary\
    .withColumn("Rank",F.rank().over(window_spec))\
        .withColumn("Dense_Rank",F.dense_rank().over(window_spec))\
            .withColumn("Row_Number",F.row_number().over(window_spec))
rank.show()



spark.stop()



