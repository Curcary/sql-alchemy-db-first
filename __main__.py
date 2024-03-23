from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, inspect
import sys

class Importer:
    def __init__(self, connstring):
        self.connstring = connstring

    def __get_column_str(self, column, primary_keys):
        map_str =""
        map_str += "primary_key=True, " if column["name"] in primary_keys else ""
        col_type = column['type'].__str__().lower()
        if "varchar" in col_type or "text" in col_type:
            col_type = "str"
        if "int" in col_type:
            col_type = "int"
        if "timestamp" in col_type:
            col_type = "datetime.datetime"
        if "date" in col_type:
            col_type = "datetime.date"
        if "boolean" in col_type:
            col_type = "bool"
        if "bytea" in col_type:
            col_type = "bytes"
        if "money" in col_type or "double" in col_type or "float" in col_type:
            col_type = "float"            
        col_str = f"\t{column['name']}: Mapped[{col_type}]= mapped_column({map_str})\n"
        return col_str

    def build(self):
        try:
            eng = create_engine(self.connstring)
            print("Connecting to database")
            insp = inspect(eng)
            relationships = []
            for tn in insp.get_table_names():
                for fk in insp.get_foreign_keys(tn):
                    relationships.append(dict(fk, table= tn))
            models_str = """from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship\n"""
            models_str+="from sqlalchemy import ForeignKeyConstraint\n"
            models_str+="import datetime\n"
            models_str+="class Base(DeclarativeBase):\n"
            models_str+="\tpass\n\n"
            print("Forming classes")
            for table_name in insp.get_table_names():
                models_str+=f"class {table_name}(Base):\n"
                models_str+=f"\t__tablename__= \"{table_name}\"\n"
                primary_keys = insp.get_pk_constraint(table_name=table_name)["constrained_columns"]
                foreign_keys = insp.get_foreign_keys(table_name=table_name)
                for column in insp.get_columns(table_name):
                    models_str+= self.__get_column_str(column, primary_keys)
                for relationship in relationships:
                    if table_name == relationship['referred_table']:
                        models_str+=f"\t{relationship['table']}_list:Mapped[list[\"{relationship['table']}\"]]"
                        models_str+= f"= relationship(viewonly = True)\n"
                for fk in foreign_keys:
                    models_str+=f"\t{fk['referred_table']}_item: Mapped[\"{fk['referred_table']}\"] = relationship(backref = \"{table_name}\")\n"
                models_str+="\t__table_args__ = ("
                for fk in foreign_keys:
                    models_str+=f"ForeignKeyConstraint({str(fk['constrained_columns'])}, "
                    rf = [str(fk["referred_table"])+'.'+ col for col in fk["referred_columns"]]
                    models_str+=str(rf)+"), "
                models_str+=")\n"
            with open("models.py", 'w') as file:
                file.write(models_str)
            print("Done")
        except IndexError:
            print("Enter connection string")
            print("Correct connection string example is \"postgresql+psycopg://postgres:password@127.0.0.1:1234/Database\"")

if __name__=="__main__":
    importer = Importer(sys.orig_argv[2])
    importer.build()