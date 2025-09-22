package godynamo

import (
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/btnguyen2k/consu/reddo"
)

func Test_parseConnString_parseParamValue(t *testing.T) {
	testName := "Test_parseConnString_parseParamValue"
	type paramValueStruct struct {
		pnames        []string
		enames        []string
		paramType     reflect.Type
		defaultValue  interface{}
		expectedValue interface{}
		validator     func(val interface{}) bool
	}

	_ = os.Setenv("ENV_TIMEOUT", "1234")
	_ = os.Setenv("ENV_TIMEOUT_INVALID", "-123")

	testCases := []struct {
		name           string
		connStr        string
		expectedParams map[string]string
		paramValues    []paramValueStruct
	}{
		{
			name:    "endpoint",
			connStr: "endpoint=http://localhost:8000",
			expectedParams: map[string]string{
				"ENDPOINT": "http://localhost:8000",
			},
			paramValues: []paramValueStruct{
				{
					pnames:        []string{"EP", "ENDPOINT"},
					paramType:     reddo.TypeString,
					expectedValue: "http://localhost:8000",
				},
				{
					pnames:        []string{"TIMEOUT"},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(123),
					expectedValue: int64(123),
				},
				{
					pnames: []string{"TIMEOUT_ENV"},
					enames: []string{"ENV_TIMEOUT"},
					validator: func(val interface{}) bool {
						return val.(int64) >= 0
					},
					paramType:     reddo.TypeInt,
					expectedValue: int64(1234),
				},
				{
					pnames: []string{"TIMEOUT_DEFAULT"},
					enames: []string{"ENV_TIMEOUT_INVALID"},
					validator: func(val interface{}) bool {
						return val.(int64) >= 0
					},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(12345),
					expectedValue: int64(12345),
				},
			},
		},
		{
			name:    "empty",
			connStr: "endpoint=http://localhost:8000;timeout",
			expectedParams: map[string]string{
				"ENDPOINT": "http://localhost:8000",
				"TIMEOUT":  "",
			},
			paramValues: []paramValueStruct{
				{
					pnames:        []string{"T", "TIMEOUT"},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(1234),
					expectedValue: int64(1234),
				},
			},
		},
		{
			name:    "invalid_timeout_value",
			connStr: "endpoint=http://localhost:8000;timeout=-1",
			expectedParams: map[string]string{
				"ENDPOINT": "http://localhost:8000",
				"TIMEOUT":  "-1",
			},
			paramValues: []paramValueStruct{
				{
					pnames:        []string{"T", "TIMEOUT"},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(1234),
					expectedValue: int64(1234),
					validator: func(val interface{}) bool {
						return val.(int64) >= 0
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			params := parseConnString(testCase.connStr)
			if !reflect.DeepEqual(params, testCase.expectedParams) {
				t.Fatalf("%s failed: expected %#v received %#v", testName+"/"+testCase.name, testCase.expectedParams, params)
			}
			for _, paramValue := range testCase.paramValues {
				val := parseParamValue(params, paramValue.paramType, paramValue.validator, paramValue.defaultValue, paramValue.pnames, paramValue.enames)
				if !reflect.DeepEqual(val, paramValue.expectedValue) {
					t.Fatalf("%s failed: <%s> expected %#v received %#v", testName+"/"+testCase.name, paramValue.pnames, paramValue.expectedValue, val)
				}
			}
		})
	}
}

func TestDriverMultipleConfigs(t *testing.T) {
	// Test registering and using AWS config
	configID := "test-config-1"
	testConfig := aws.Config{
		Region: "us-west-2",
	}

	// Register the config
	RegisterAWSConfig(configID, testConfig)

	// Verify the config is stored
	awsConfigLock.RLock()
	storedConfig, exists := awsConfig[configID]
	awsConfigLock.RUnlock()

	if !exists {
		t.Fatal("Config should be registered")
	}
	if storedConfig.Region != testConfig.Region {
		t.Fatalf("Expected region %s, got %s", testConfig.Region, storedConfig.Region)
	}

	// Test using the registered config with only the config ID in connection string
	driver := &Driver{}
	connStr := "aws_config_id=" + configID

	conn, err := driver.Open(connStr)
	if err != nil {
		t.Fatalf("Expected successful connection with registered config, got error: %v", err)
	}
	if conn == nil {
		t.Fatal("Expected valid connection")
	}

	// Test deregistering the config
	DeregisterAWSConfig(configID)

	// Verify the config is removed
	awsConfigLock.RLock()
	_, exists = awsConfig[configID]
	awsConfigLock.RUnlock()

	if exists {
		t.Fatal("Config should be deregistered")
	}

	// Test that using deregistered config returns error
	_, err = driver.Open(connStr)
	if err != ErrUnknownAWSConfigID {
		t.Fatalf("Expected ErrUnknownAWSConfigID, got: %v", err)
	}

	// Test fallback to regular client when no AWS_CONFIG_ID is provided
	connStrNoConfig := "region=us-east-1;akid=test;secret_key=test"
	conn2, err := driver.Open(connStrNoConfig)
	if err != nil {
		t.Fatalf("Expected successful connection without config ID, got error: %v", err)
	}
	if conn2 == nil {
		t.Fatal("Expected valid connection without config ID")
	}
}
