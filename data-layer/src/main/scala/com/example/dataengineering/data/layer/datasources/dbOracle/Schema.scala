package com.example.dataengineering.data.layer.datasources.dbOracle
import java.time.LocalDateTime
import com.example.dataengineering.data.layer.schemas.LoaderSchema
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
case class DEPARTMENTS(DEPARTMENT_ID: String,
                       DEPARTMENT_NAME: String,
                       MANAGER_ID: String,
                       LOCATION_ID: String)
    extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
};

case class EMPLOYEES(EMPLOYEE_ID: String,
                     FIRST_NAME: String,
                     LAST_NAME: String,
                     EMAIL: String,
                     PHONE_NUMBER: String,
                     HIRE_DATE: String,
                     JOB_ID: String,
                     SALARY: String,
                     COMMISSION_PCT: String,
                     MANAGER_ID: String,
                     DEPARTMENT_ID: String)
    extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
};

case class JOBS(JOB_ID: String,
                JOB_TITLE: String,
                MIN_SALARY: String,
                MAX_SALARY: String)
    extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
};

case class JOB_HISTORY(EMPLOYEE_ID: String,
                       START_DATE: String,
                       END_DATE: String,
                       JOB_ID: String,
                       DEPARTMENT_ID: String)
    extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
};

case class LOCATIONS(LOCATION_ID: String,
                     STREET_ADDRESS: String,
                     POSTAL_CODE: String,
                     CITY: String,
                     STATE_PROVINCE: String,
                     COUNTRY_ID: String)
    extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
};

case class REGIONS(REGION_ID: String, REGION_NAME: String)
    extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
};

case class COUNTRIES(COUNTRY_ID: String,
                     COUNTRY_NAME: String,
                     REGION_ID: String)
    extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
};
