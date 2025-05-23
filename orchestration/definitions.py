# ==================== #
#                      #
#        Set Up        #
#                      #
# ==================== #

from pathlib import Path
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
import dagster as dg
 
# data warehouse directory
db_path = str(Path(__file__).parents[1] / "data_warehouse/ads.duckdb")


# ==================== #
#                      #
#       dbt Asset      #
#                      #
# ==================== #

# Points to the dbt project path
dbt_project_directory = Path(__file__).parents[1] / "data_transformation"
# Define the path to your profiles.yml file (in your home directory)
profiles_dir = Path.home() / ".dbt"  
dbt_project = DbtProject(project_dir=dbt_project_directory,
                         profiles_dir=profiles_dir)
# References the dbt project object
dbt_resource = DbtCliResource(project_dir=dbt_project)
# Compiles the dbt project & allow Dagster to build an asset graph
dbt_project.prepare_if_dev()


# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path,) #access metadata of dbt project so that dagster understand structure of the dbt project
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream() #compile the project and collect all results


# ==================== #
#                      #
#     Definitions      #
#                      #
# ==================== #

# Dagster object that contains the dbt assets and resource
defs = dg.Definitions(
                    assets=[dbt_models], 
                    resources={"dbt": dbt_resource},
                    )


