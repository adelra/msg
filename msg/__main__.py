from .broker import SimpleMessageBroker
import asyncio
import argparse

def main():
    parser = argparse.ArgumentParser(description="Run the SimpleMessageBroker server.")
    parser.add_argument("--host", type=str, default="localhost", help="Host to bind the server to (default: localhost)")
    parser.add_argument("--port", type=int, default=8888, help="Port to listen on (default: 8888)")
    args = parser.parse_args()

    broker = SimpleMessageBroker(host=args.host, port=args.port)
    asyncio.run(broker.start())

if __name__ == "__main__":
    main()
