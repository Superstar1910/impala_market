"""Refresh public RSS link metadata; preserve the last good cache on failure."""
import argparse
import json
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
FEED = 'https://legesi.substack.com/feed'

def parse_feed(data):
    root = ET.fromstring(data)
    if root.tag != 'rss':
        raise ValueError('Expected an RSS document, not an HTML error page')
    articles=[]
    for item in root.findall('./channel/item'):
        title=item.findtext('title','').strip()
        url=item.findtext('link','').strip()
        description=item.findtext('description','')
        if not re.search(r'\bMTN\b|\bMoMo\b',title+' '+description,re.I):
            continue
        parsed=urlparse(url)
        if parsed.scheme!='https' or parsed.hostname!='legesi.substack.com' or not parsed.path.startswith('/p/'):
            continue
        articles.append({'title':title,'url':url,'date':item.findtext('pubDate','')})
    return articles

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--feed-file',type=Path,help='Local RSS fixture for offline checks')
    args=parser.parse_args()
    try:
        if args.feed_file:
            data=args.feed_file.read_bytes()
        else:
            request=urllib.request.Request(FEED,headers={'User-Agent':'LegesiResearchRSS/1.0'})
            with urllib.request.urlopen(request,timeout=30) as response:
                data=response.read(4_000_001)
        if len(data)>4_000_000: raise ValueError('Feed too large')
        articles=parse_feed(data)
        if args.feed_file:
            print(json.dumps({'articles':articles},ensure_ascii=False))
            return
        output={'source':FEED,'fetched_at':datetime.now(timezone.utc).isoformat(),'articles':articles}
        cache=ROOT/'docs/substack-articles.json'
        pending=cache.with_suffix('.tmp')
        pending.write_text(json.dumps(output,ensure_ascii=False,indent=2),encoding='utf-8')
        pending.replace(cache)
        print(f'Refreshed {len(articles)} article links; curated history remains separate.')
    except Exception as error:
        print(f'RSS refresh unavailable; prior cache and curated links preserved: {error}',file=sys.stderr)
        sys.exit(1)

if __name__=='__main__': main()
