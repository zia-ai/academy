How to mount S3

Install AWS

    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    sudo apt-get install unzip
    unzip awscliv2.zip
    sudo ./aws/install

Authenticate
get credentials - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION from 1Password and save as .env
The .env file should have the values stored in following format:

```
AWS_ACCESS_KEY_ID=<aws access key>
AWS_SECRET_ACCESS_KEY=<aws secret access key>
AWS_DEFAULT_REGION=<region>
```

export $(cat .env | xargs)
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html


https://docs.aws.amazon.com/cli/latest/userguide/cli-services-s3-commands.html


    aws s3 ls

    aws s3 cp <path> ./data

