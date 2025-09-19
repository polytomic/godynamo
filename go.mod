module github.com/btnguyen2k/godynamo

go 1.22

toolchain go1.24.6

require (
	github.com/aws/aws-sdk-go-v2 v1.39.0
	github.com/aws/aws-sdk-go-v2/credentials v1.17.52
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.11
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.50.3
	github.com/aws/smithy-go v1.23.0
	github.com/btnguyen2k/consu/g18 v0.1.0
	github.com/btnguyen2k/consu/reddo v0.1.9
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.30.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.7 // indirect
)
