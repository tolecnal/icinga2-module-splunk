#!/usr/bin/env python3
"""
Test connection to Icinga2 API, Splunk HEC, and Event Streams
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
import json
from config import Config

# Disable SSL warnings if verify_ssl is False
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def test_icinga2_api(config):
    """Test basic Icinga2 API connectivity using /v1/status endpoint"""
    print("\n=== Testing Icinga2 API Connection ===")
    
    url = f"{config.icinga2_api_url}/v1"
    
    try:
        auth = None
        cert = None
        
        if config.icinga2_api_username and config.icinga2_api_password:
            auth = (config.icinga2_api_username, config.icinga2_api_password)
        
        if config.icinga2_api_client_cert and config.icinga2_api_client_key:
            cert = (config.icinga2_api_client_cert, config.icinga2_api_client_key)
        
        response = requests.get(
            url,
            auth=auth,
            cert=cert,
            verify=config.icinga2_api_verify_ssl,
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"✓ Icinga2 API connection successful")
            print(f"  API endpoint: {url}")
            
            status_url = f"{config.icinga2_api_url}/v1/status"
            status_response = requests.get(
                status_url,
                auth=auth,
                cert=cert,
                verify=config.icinga2_api_verify_ssl,
                timeout=10
            )
            
            if status_response.status_code == 200:
                data = status_response.json()
                if 'results' in data and len(data['results']) > 0:
                    results = data['results'][0]
                    if 'status' in results:
                        status = results['status']
                        print(f"  - Icinga2 Version: {status.get('version', 'unknown')}")
                        uptime = status.get('uptime', 0)
                        print(f"  - Uptime: {int(uptime)} seconds ({int(uptime/3600)} hours)")
            
            return True
        else:
            print(f"✗ Failed to connect: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Connection error: {str(e)}")
        print(f"  Make sure Icinga2 API is running on {config.icinga2_api_url}")
        return False
    except requests.exceptions.Timeout:
        print(f"✗ Connection timeout after 10 seconds")
        return False
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


def test_icinga2_hosts(config):
    """Test fetching hosts from Icinga2 API"""
    print("\n=== Testing Icinga2 Hosts Query ===")
    
    url = f"{config.icinga2_api_url}/v1/objects/hosts"
    
    try:
        auth = None
        cert = None
        
        if config.icinga2_api_username and config.icinga2_api_password:
            auth = (config.icinga2_api_username, config.icinga2_api_password)
        
        if config.icinga2_api_client_cert and config.icinga2_api_client_key:
            cert = (config.icinga2_api_client_cert, config.icinga2_api_client_key)
        
        response = requests.get(
            url,
            auth=auth,
            cert=cert,
            verify=config.icinga2_api_verify_ssl,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            host_count = len(data.get('results', []))
            print(f"✓ Successfully retrieved {host_count} host(s)")
            
            # Show first few hosts
            if host_count > 0:
                print("  Sample hosts:")
                for host in data['results'][:3]:
                    attrs = host.get('attrs', {})
                    print(f"    - {attrs.get('name', 'unknown')}: {attrs.get('state_type', 'unknown')} state")
            else:
                print("  ℹ No hosts configured yet (this is normal for a new installation)")
            
            return True
        elif response.status_code == 404:
            print(f"✗ No hosts found or missing permissions")
            print(f"  This might be normal if:")
            print(f"    1. No hosts are configured yet")
            print(f"    2. API user lacks 'objects/query/Host' permission")
            print(f"  Response: {response.text}")
            return False
        else:
            print(f"✗ Failed to fetch hosts: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


def test_event_stream(config):
    """Test Event Stream connectivity"""
    print("\n=== Testing Event Stream Connection ===")
    
    url = f"{config.icinga2_api_url}/v1/events"
    
    try:
        auth = None
        cert = None
        
        if config.icinga2_api_username and config.icinga2_api_password:
            auth = (config.icinga2_api_username, config.icinga2_api_password)
        
        if config.icinga2_api_client_cert and config.icinga2_api_client_key:
            cert = (config.icinga2_api_client_cert, config.icinga2_api_client_key)
        
        # Subscribe to events with a short timeout
        payload = {
            "queue": "test-connection",
            "types": ["CheckResult", "StateChange"]
        }
        
        headers = {
            "Accept": "application/json"
        }
        
        response = requests.post(
            url,
            auth=auth,
            cert=cert,
            json=payload,
            headers=headers,
            verify=config.icinga2_api_verify_ssl,
            timeout=5,
            stream=True
        )
        
        if response.status_code == 200:
            print(f"✓ Event Stream connection successful")
            print(f"  Stream is ready to receive events")
            
            # Try to read one event
            print(f"  Attempting to read an event (5 second timeout)...")
            try:
                for line in response.iter_lines(decode_unicode=True):
                    if line:
                        print(f"  ✓ Received event data")
                        break
            except requests.exceptions.ReadTimeout:
                print(f"  ℹ No events received within timeout (this is normal if no checks are running)")
            
            return True
        else:
            print(f"✗ Failed to connect to event stream: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"✗ Connection timeout")
        return False
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


def test_splunk_hec(config):
    """Test Splunk HEC connectivity"""
    print("\n=== Testing Splunk HEC Connection ===")
    
    base_url = config.splunk_hec_url.rstrip('/')
    if base_url.endswith('/services/collector'):
        url = f"{base_url}/event"
    else:
        url = f"{base_url}/services/collector/event"
    
    print(f"  Testing URL: {url}")
    
    try:
        test_event = {
            "event": "Test event from Icinga2-Splunk connector",
            "sourcetype": "icinga2:test",
            "source": "test_connection.py"
        }
        
        headers = {
            "Authorization": f"Splunk {config.splunk_hec_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            url,
            json=test_event,
            headers=headers,
            verify=config.splunk_verify_ssl,
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"✓ Splunk HEC connection successful")
            result = response.json()
            print(f"  Response: {result}")
            return True
        else:
            print(f"✗ Failed to send to Splunk: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            if response.status_code == 404:
                print(f"  Hint: Check that your splunk_hec_url is correct.")
                print(f"        It should be: https://your-splunk-server:8088")
                print(f"        NOT: https://your-splunk-server:8088/services/collector")
            elif response.status_code == 401 or response.status_code == 403:
                print(f"  Hint: Check that your HEC token is valid and not disabled")
            return False
            
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Connection error: {str(e)}")
        print(f"  Make sure Splunk HEC is running on {base_url}")
        return False
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


def main():
    print("Icinga2 to Splunk Integration - Connection Test")
    print("=" * 50)
    
    try:
        config = Config()
        print("✓ Configuration loaded successfully")
    except Exception as e:
        print(f"✗ Failed to load configuration: {str(e)}")
        sys.exit(1)
    
    # Run all tests
    results = {
        "Icinga2 API": test_icinga2_api(config),
        "Icinga2 Hosts": test_icinga2_hosts(config),
        "Event Stream": test_event_stream(config),
        "Splunk HEC": test_splunk_hec(config)
    }
    
    # Summary
    print("\n" + "=" * 50)
    print("Test Summary:")
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {test_name}: {status}")
    
    all_passed = all(results.values())
    if all_passed:
        print("\n✓ All tests passed! Ready to start the Event Stream client.")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed. Please check the configuration and errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

