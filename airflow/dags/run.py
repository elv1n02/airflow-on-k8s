import great_expectations as gx
import os

os.makedirs('/opt/gx-dev', exist_ok = True)

# DAG_DIR = os.path.dirname(os.path.abspath(__file__))

# context = gx.get_context(mode='file', project_root_dir=DAG_DIR)
# print(context.checkpoints.all())

# checkpoint = context.checkpoints.get("my_checkpoint")

# validation_results = checkpoint.run()

# print(validation_results)
