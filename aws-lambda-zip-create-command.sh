pip install --platform manylinux2014_x86_64 --target lambda-function-code --implementation cp --python 3.10 --only-binary :all: --upgrade pymongo boto3 requests