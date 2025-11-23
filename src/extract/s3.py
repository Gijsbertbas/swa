import boto3

from util import ETLConfig


class S3Extractor:
    def __init__(self, aws_profile: str, bucket_name: str, config: ETLConfig):
        self.bucket_name = bucket_name
        self.config = config
        self.s3_client = boto3.session.Session(profile_name=aws_profile).client('s3')
        self.filenames = []

    @property
    def output_file(self) -> str:
        return f'{self.config.type.value}.txt'

    def collect_filenames(self):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.config.s3_prefix
        )

        filenames = []
        total_count = 0

        print(f"Collecting files from s3://{self.bucket_name}/{self.config.s3_prefix}")

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    filename = key.split('/')[-1]

                    if filename:
                        filenames.append(filename)
                        total_count += 1

        self.filenames = filenames
        print(f'Collected {total_count} files')

    def generate_rclone_filter_list(self) -> str:
        with open(self.output_file, 'w') as f:
            for filename in self.filenames:
                f.write(f"- {filename}\n")
            f.write(f'+ {self.config.filename_prefix}*\n')
            f.write(f'- *')

        print(f"Filter list saved to: {self.output_file}")
        return self.output_file
