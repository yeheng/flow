#!/usr/bin/env python3
"""Minimal conga external tool: list + echo over stdin/stdout JSONL."""
import json
import sys


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        msg = json.loads(line)
        op = msg.get("op")
        if op == "list":
            print(
                json.dumps(
                    {
                        "tools": [
                            {
                                "name": "echo",
                                "description": "Echo back text",
                                "parameters": {
                                    "type": "object",
                                    "properties": {
                                        "text": {"type": "string", "description": "text to echo"}
                                    },
                                    "required": ["text"],
                                },
                            }
                        ]
                    }
                ),
                flush=True,
            )
        elif op == "call":
            text = (msg.get("args") or {}).get("text", "")
            print(
                json.dumps(
                    {
                        "id": msg.get("id", ""),
                        "content": [{"type": "text", "text": str(text)}],
                        "is_error": False,
                    }
                ),
                flush=True,
            )
        else:
            print(
                json.dumps(
                    {
                        "id": msg.get("id", ""),
                        "content": [{"type": "text", "text": f"unknown op: {op}"}],
                        "is_error": True,
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    main()
