# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AWSLambdaInvokeOperator(BaseOperator):
    """
    Invokes an AWS lambda function



    :param function_name: The name of the lambda function to invoke.
    :type function_name: str

    :param invocation_type: The type of invocation to commit.

        Can be of type 'Event'|'RequestResponse'|'DryRun', default is 'DryRun'.

    :type invocation_type: str

    """

    @apply_defaults
    def __init__(
            self,
            function_name=None,
            invocation_type=None,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super(AWSLambdaInvokeOperator, self).__init__(*args, **kwargs)

        self.source_bucket_key = source_bucket_key
        self.dest_bucket_key = dest_bucket_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.source_version_id = source_version_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_hook.copy_object(self.source_bucket_key, self.dest_bucket_key,
                            self.source_bucket_name, self.dest_bucket_name,
                            self.source_version_id)
        response = client.invoke(
            FunctionName='string',
            InvocationType='Event'|'RequestResponse'|'DryRun',
            LogType='None'|'Tail',
            ClientContext='string',
            Payload=b'bytes'|file,
            Qualifier='string')
