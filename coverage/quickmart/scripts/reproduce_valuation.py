"""Reproduce the published workbook's provisional arithmetic; standard library only.

This is a calculation check, not economic validation. Units: KES billion.
Run from the repository: python3 scripts/reproduce_valuation.py
"""
import json, csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
d = json.loads((ROOT/'valuation/forecast-inputs.json').read_text())
t=d['tax'];r=d['wacc'];g=d['terminal_growth'];shares=d['shares_bn']
fcff=[p['ebit']*(1-t)+p['ppe_intangible_da']+p['rou_depreciation']-p['capex']-p['nwc_outflow'] for p in d['periods']]
rent_cf=[cf-t*p['rou_depreciation']-(1-t)*p['rental_proxy'] for cf,p in zip(fcff,d['periods'])]

def valuation(cash,r,g,net_debt):
    if r<=g: raise ValueError('WACC must exceed perpetual growth')
    pv=sum(c/(1+r)**n for n,c in enumerate(cash,1))
    tv=cash[-1]*(1+g)/(r-g)/(1+r)**len(cash)
    ev=pv+tv;equity=ev-net_debt
    return dict(pv_explicit_bn=pv,pv_terminal_bn=tv,enterprise_value_bn=ev,net_debt_bn=net_debt,equity_value_bn=equity,value_per_share=equity/shares,upside=equity/shares/d['ipo_price']-1)

post=valuation(fcff,r,g,d['net_debt_including_leases_bn'])
pre=valuation(rent_cf,r,g,d['net_debt_including_leases_bn']-d['lease_liabilities_bn'])
assert abs(post['value_per_share']-8.652297166949285)<1e-8
assert abs(pre['value_per_share']-6.750578143528876)<1e-8
result={'status':d['status'],'model_anchor':d['anchor'],'post_ifrs16':post,'rental_proxy_cross_check':pre,'difference_per_share':post['value_per_share']-pre['value_per_share'],'warning':'Arithmetic reproduced; lease renewal/additions, terminal reinvestment, WACC and valuation date remain unresolved.'}
(ROOT/'valuation/reproduced-results.json').write_text(json.dumps(result,indent=2)+'\n')
gs=[.025,.03,.04,.05,.055];rs=[.145,.15,.16,.17,.18]
for filename,key in [('enterprise-value-sensitivity.csv','enterprise_value_bn'),('equity-per-share-sensitivity.csv','value_per_share')]:
    with (ROOT/'valuation'/filename).open('w',newline='') as out:
        writer=csv.writer(out);writer.writerow(['WACC / growth']+gs)
        for rate in rs:writer.writerow([rate]+[round(valuation(fcff,rate,growth,d['net_debt_including_leases_bn'])[key],6) for growth in gs])
with (ROOT/'valuation/forecast-cashflows.csv').open('w',newline='') as out:
    writer=csv.writer(out);writer.writerow(['year','post_IFRS16_FCFF_KES_bn','rental_proxy_FCFF_KES_bn'])
    for p,a,b in zip(d['periods'],fcff,rent_cf):writer.writerow([p['year'],a,b])

# Exact code-native dashboard: all sensitivities calculated, no generated chart numbers.
def txt(x,y,s,size=24,color='#16324f',weight='normal'):
    from html import escape
    return f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" font-weight="{weight}">{escape(s)}</text>'
svg=['<svg xmlns="http://www.w3.org/2000/svg" width="1400" height="1000" viewBox="0 0 1400 1000"><rect width="1400" height="1000" fill="#fffdf7"/><g font-family="Arial,sans-serif">']
svg += [txt(55,72,'QUICKMART IPO: WHAT IS A SHARE WORTH?',40,weight='bold'),txt(55,115,'Provisional research | 12 October 2026 | Model anchor: end-2026',22)]
for x,title,value,sub,col in [(55,'P/E screening','KES 6.83','Illustrative 13x FY2026 earnings','#e6f2fa'),(495,'IPO offer price','KES 7.50','2bn existing shares offered','#ffebe9'),(935,'Provisional DCF','KES 8.65','16% WACC / 4% terminal growth','#e5f4e9')]:
    svg.append(f'<rect x="{x}" y="150" width="410" height="185" rx="16" fill="{col}"/>')
    svg += [txt(x+20,190,title,26,weight='bold'),txt(x+20,258,value,46,weight='bold'),txt(x+20,304,sub,19)]
svg.append(txt(55,392,'ENTERPRISE-TO-EQUITY BRIDGE (KES bn)',27,weight='bold'))
for n,(label,value) in enumerate([('PV forecast cash flow','19.02'),('PV terminal value','22.86'),('Enterprise value','41.88'),('Less net obligations, including leases','(7.27)'),('Equity value','34.61'),('Shares outstanding (bn)','4.00'),('Indicative value per share (KES)','8.65')]):
    y=443+43*n;svg += [txt(55,y,label,21),txt(555,y,value,24,weight='bold')]
svg.append(txt(750,392,'EXACT SHARE VALUE SENSITIVITY',25,weight='bold'))
svg.append(txt(750,430,'WACC / g',20,weight='bold'))
for j,growth in enumerate(gs):svg.append(txt(910+j*90,430,f'{growth*100:g}%',20,weight='bold'))
for k,rate in enumerate(rs):
    y=472+k*43;svg.append(txt(750,y,f'{rate*100:g}%',21))
    for j,growth in enumerate(gs):svg.append(txt(910+j*90,y,f"{valuation(fcff,rate,growth,d['net_debt_including_leases_bn'])['value_per_share']:.2f}",22))
svg += [txt(55,815,'Rental-proxy cross-check: KES 6.75 | Unresolved gap: KES 1.90/share',27,weight='bold'),txt(55,866,'Illustrative valuation; lease reconciliation and market calibration outstanding.',23,color='#a02d25',weight='bold'),txt(55,910,'Terminal reinvestment and the October valuation-date stub also remain unresolved.',21),txt(55,955,'Source: revised workbook. Arithmetic independently reproduced; not a validated price target.',20)]
svg.append('</g></svg>');(ROOT/'graphics/dcf-valuation-dashboard.svg').write_text(''.join(svg))
print(json.dumps(result,indent=2))
