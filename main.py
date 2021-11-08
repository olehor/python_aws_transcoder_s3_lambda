import json
import os
import boto3


def lambda_handler(event, context):
    pipeline_id = 'xxxxxxxxxxxxxxxxxxxxx'
    video_h264_preset_id = 'xxxxxxxxxxxxxxxxxxxxx'

    transcoder_client = boto3.client('elastictranscoder')

    # s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    out = []
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # check if this object is original
        if key.startswith("transcoded__"):
            original_filename = key.split("transcoded__")[1]
            # Delete original video file
            # s3_client.delete_object(Bucket=bucket, Key=original_filename)
            s3_resource.Object(bucket, original_filename).delete()

            # Rename generated H.264 video as original video name
            source_object = f"/{bucket}/{key}"
            # s3_client.copy_object(Bucket=bucket, CopySource=source_object, Key=original_filename)
            s3_resource.Object(bucket, original_filename).copy_from(CopySource=source_object, ACL='public-read')

            # Delete generated H.264 video
            # s3_client.delete_object(Bucket=bucket, Key=key)
            s3_resource.Object(bucket, key).delete()
        else:
            # Strip the extension, Elastic Transcoder automatically adds one
            converted_filename = "transcoded__" + os.path.splitext(key)[0] + '.mp4'

            outputs = [
                {
                    'Key': converted_filename,
                    'PresetId': video_h264_preset_id,
                }
            ]
            response = transcoder_client.create_job(PipelineId=pipeline_id,
                                                    Input={'Key': key},
                                                    Outputs=outputs,
                                                    )

            print(response)
            out.append(response)

    return {
        'statusCode': 200,
        'body': out
    }
