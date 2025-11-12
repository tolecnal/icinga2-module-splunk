#!/usr/bin/env python3
"""
Test script to verify Icinga2 and Splunk connections
"""

import sys
import json
import logging
import requests
from requests.auth import HTTPBasicAuth

from config import Config
from splunk_hec_client import SplunkHECClient


def test_icinga2_connection(config: Config) -> bool:
    """Test connection to Icinga2 API"""
    print("\n=== Testing Icinga2 API Connection ===")

    try:
        url = f"{config.icinga2_api_url}/v1/status"

        auth = None
        cert = None

        if config.icinga2_api_username and config.icinga2_api_password:
            auth = HTTPBasicAuth(
                config.icinga2_api_username, config.icinga2_api_password
            )

        if config.icinga2_api_client_cert and config.icinga2_api_client_key:
            cert = (config.icinga2_api_client_cert, config.icinga2_api_client_key)

        response = requests.get(
            url, auth=auth, cert=cert, verify=config.icinga2_api_verify_ssl, timeout=10
        )

        if response.status_code == 200:
            status = response.json()
            print(f"✓ Successfully connected to Icinga2 API")
            print(
                f"  Version: {status['results'][0]['status']['icingaapplication']['app']['version']}"
            )
            print(f"  Node: {status['results'][0]['name']}")
            return True
        else:
            print(f"✗ Failed to connect: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False

    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False


def test_splunk_connection(config: Config) -> bool:
    """Test connection to Splunk HEC"""
    print("\n=== Testing Splunk HEC Connection ===")

    try:
        # Test event
        test_event = {
            "event": {
                "test": "connection",
                "message": "Test event from Icinga2 Splunk integration",
            },
            "sourcetype": "icinga2:test",
        }

        response = requests.post(
            config.splunk_hec_url,
            headers={
                "Authorization": f"Splunk {config.splunk_hec_token}",
                "Content-Type": "application/json",
            },
            json=test_event,
            verify=config.splunk_verify_ssl,
            timeout=10,
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✓ Successfully sent test event to Splunk")
            print(f"  Response: {result}")
            return True
        else:
            print(f"✗ Failed to send event: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False

    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False


def test_event_stream(config: Config) -> bool:
    """Test Icinga2 Event Stream subscription"""
    print("\n=== Testing Icinga2 Event Stream ===")

    try:
        url = f"{config.icinga2_api_url}/v1/events"

        payload = {
            "queue": "test-queue",
            "types": ["CheckResult"],
            "filter": "event.check_result.state==0",
        }

        auth = None
        cert = None

        if config.icinga2_api_username and config.icinga2_api_password:
            auth = HTTPBasicAuth(
                config.icinga2_api_username, config.icinga2_api_password
            )

        if config.icinga2_api_client_cert and config.icinga2_api_client_key:
            cert = (config.icinga2_api_client_cert, config.icinga2_api_client_key)

        print("  Subscribing to event stream (will wait 5 seconds for events)...")

        with requests.post(
            url,
            json=payload,
            auth=auth,
            cert=cert,
            verify=config.icinga2_api_verify_ssl,
            stream=True,
            timeout=10,
            headers={"Accept": "application/json"},
        ) as response:
            if response.status_code != 200:
                print(f"✗ Failed to subscribe: HTTP {response.status_code}")
                print(f"  Response: {response.text}")
                return False

            print(f"✓ Successfully subscribed to event stream")

            # Try to read a few events
            event_count = 0
            import time

            start_time = time.time()

            for line in response.iter_lines():
                if time.time() - start_time > 5:  # Wait 5 seconds
                    break

                if line:
                    try:
                        event = json.loads(line.decode("utf-8"))
                        event_count += 1
                        print(f"  Received event: {event.get('type', 'Unknown')}")

                        if event_count >= 3:  # Show max 3 events
                            break
                    except:
                        pass

            if event_count > 0:
                print(f"✓ Received {event_count} events from stream")
            else:
                print(f"  No events received (this is normal if no checks are running)")

            return True

    except Exception as e:
        print(f"✗ Event stream error: {e}")
        return False


def main():
    """Main test function"""
    print("Icinga2 to Splunk Integration - Connection Test")
    print("=" * 50)

    # Setup logging
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        # Load configuration
        config = Config("/etc/icinga2/splunk_config.json")
        print("✓ Configuration loaded successfully")

        # Run tests
        icinga2_ok = test_icinga2_connection(config)
        splunk_ok = test_splunk_connection(config)
        stream_ok = test_event_stream(config)

        # Summary
        print("\n=== Test Summary ===")
        print(f"Icinga2 API:     {'✓ PASS' if icinga2_ok else '✗ FAIL'}")
        print(f"Splunk HEC:      {'✓ PASS' if splunk_ok else '✗ FAIL'}")
        print(f"Event Stream:    {'✓ PASS' if stream_ok else '✗ FAIL'}")

        if icinga2_ok and splunk_ok and stream_ok:
            print("\n✓ All tests passed! Integration is ready to use.")
            return 0
        else:
            print("\n✗ Some tests failed. Please check the configuration.")
            return 1

    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
