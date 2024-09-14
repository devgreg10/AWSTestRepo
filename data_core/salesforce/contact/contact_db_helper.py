from data_core.salesforce.contact.contact_db_models import SfContactRawDbModel
from data_core.util.db_execute_helper import DbExecutorHelper
from psycopg2.extensions import connection

class SalesforceContactDbHelper:

    @staticmethod
    def insert_sf_raw_contact(*, 
                              db_connection: connection, 
                              new_raw_contact: SfContactRawDbModel,
                              commit_changes: bool = True, 
                              close_db_conn: bool = True):
    
        response = DbExecutorHelper.execute_function(
            db_connection=db_connection,
            db_schema="ft_ds_admin",
            db_function="write_sf_contact_sf_to_raw",
            input_parameters=new_raw_contact.as_dict(),
            commit_changes=commit_changes,
            close_db_conn=close_db_conn
        )
