import great_expectations as gx

context = gx.get_context(mode='file')
print(context.checkpoints)

checkpoint = context.checkpoints.get("my_checkpoint")

validation_results = checkpoint.run()

print(validation_results)
