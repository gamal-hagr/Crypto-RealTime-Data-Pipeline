import websocket
import json
import time
import logging
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError

# --- CONFIGURATION AREA (Coordinate with Magda) ---
# Replace with the Connection String provided by Magda (DE-1.3)
CONNECTION_STR = "Endpoint=sb://crypto-stream-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oM/JUAyIl0U38VxGPMDAd9TxSh9/Trn3S+AEhFZXNuI="
EVENT_HUB_NAME = "crypto-raw-stream"

# Setup logging to monitor the system's health
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BinanceProducer")

class CryptoProducer:
    def __init__(self, conn_str, eh_name):
        """Initialize Azure Event Hubs Producer Client."""
        try:
            self.client = EventHubProducerClient.from_connection_string(
                conn_str=conn_str, 
                eventhub_name=eh_name
            )
            logger.info("Successfully connected to Azure Event Hubs Client.")
        except Exception as e:
            logger.error(f"Failed to initialize Azure Client: {e}")
            raise

        self.socket_url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

    def send_to_azure(self, message):
        """Batch and send records to Azure to optimize throughput and cost."""
        try:
            # Creating a batch for efficient data transfer
            event_data_batch = self.client.create_batch()
            event_data_batch.add(EventData(message))
            
            # Sending the batch to the Event Hub
            self.client.send_batch(event_data_batch)
            logger.debug("Trade data successfully sent to Azure.")
        except EventHubError as e:
            logger.error(f"Azure EventHub Error: {e}")
        except Exception as e:
            logger.error(f"General Error during transmission: {e}")

    def on_message(self, ws, message):
        """Callback triggered when new trade data arrives from Binance."""
        # Process and forward data immediately
        self.send_to_azure(message)
        
        # Display short summary in console for tracking
        data = json.loads(message)
        print(f"Captured: {data['s']} | Price: {data['p']} | Quantity: {data['q']}")

    def on_error(self, ws, error):
        """Handle WebSocket connection errors."""
        logger.error(f"WebSocket Error encountered: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket closure."""
        logger.warning(f"Connection Closed. Code: {close_status_code}, Message: {close_msg}")

    def run(self):
        """Run the producer with an infinite loop for auto-reconnection (DE-1.6)."""
        while True:
            try:
                logger.info("Opening WebSocket connection to Binance...")
                ws = websocket.WebSocketApp(
                    self.socket_url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                # Keep the connection alive
                ws.run_forever()
            except Exception as e:
                # Simple exponential backoff for resilience
                logger.error(f"Connection lost: {e}. Retrying in 5 seconds...")
                time.sleep(5)

if __name__ == "__main__":
    # Ensure Member 1 (Gamal) executes this script to initiate the pipeline
    producer = CryptoProducer(CONNECTION_STR, EVENT_HUB_NAME)
    producer.run()