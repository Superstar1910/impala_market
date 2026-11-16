"""Validate generated legacy redirects without publishing upstream content."""
from pathlib import Path
R=Path(__file__).resolve().parents[1]
for name in ['index.html','valuation.html','ownership.html','substack.html']:
    slug='' if name=='index.html' else name[:-5]+'/'
    target='/work/mtn-uganda-stock-coverage/'+slug
    alias=(R/'docs'/name).read_text()
    assert f'href="https://legesi.xyz{target}"' in alias,name
    assert f'content="0;url={target}"' in alias,name
    assert 'src="/static/js/url-redirect.js"' in alias,name
    assert '<h1>' in (R/'canonical'/name).read_text(),name
print('Validated four canonical exports and four legacy redirect documents.')
