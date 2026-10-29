from pipeline import main
 
 
def lambda_handler(event, context):
    main()
    return {"statusCode": 200}

if __name__ == "__main__":
    print(lambda_handler({}, None))