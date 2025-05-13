import great_expectations as gx
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
GX_DIR = os.path.abspath(os.path.join(DAG_DIR, 'gx'))

context = gx.get_context(mode='file', project_root_dir=GX_DIR)
print(context.checkpoints.all())

checkpoint = context.checkpoints.get("my_checkpoint")

validation_results = checkpoint.run()

print(validation_results)
