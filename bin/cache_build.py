#!/usr/bin/env python3
import argparse
import math
import time
import urllib.request


def build_cache(base_url: str, start_level: int = 0, end_level: int = 10) -> int:
    """Send initial requests to build up the cache

    :param base_url: Base URL to hit
    :param start_level: Start Z level (defaults to 0)
    :param end_level: End Z level (defaults to 10)
    :return: How many tiles were cached
    """
    num_tiles_cached = 0
    for z in range(start_level, end_level + 1):
        for x in range(int(math.pow(2, z))):
            for y in range(int(math.pow(2, z))):
                url = f"{base_url}/{z}/{x}/{y}.png"
                with urllib.request.urlopen(url):
                    num_tiles_cached += 1
                    print(url)
    return num_tiles_cached

def main():
    parser = argparse.ArgumentParser(
        description="Request TMS tiles to build up the cache"
    )
    parser.add_argument(
        "base_url",
        type=str,
        help="Base URL to request without the tile z/x/y information",
    )
    parser.add_argument(
        "-l",
        "--end_level",
        type=int,
        default=10,
        help="Max level to descend to (default 10)",
    )
    parser.add_argument(
        "-s",
        "--start_level",
        type=int,
        default=0,
        help="Min level to descend to (default 0)",
    )
    args = parser.parse_args()

    s1 = time.time()
    num_tiles = build_cache(
        args.base_url,
        start_level=args.start_level,
        end_level=args.end_level
    )
    duration = time.time() - s1
    per_tile = duration / num_tiles
    print(f"Took {duration} sec for {num_tiles} tiles ({per_tile} s/tile)")

if __name__ == "__main__":
    main()
