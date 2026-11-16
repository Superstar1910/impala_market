const oil=document.querySelector('#oil'),fx=document.querySelector('#fx'),reserves=document.querySelector('#reserves');
const fmt=n=>new Intl.NumberFormat('en-GB').format(n);
function updateScenario(){
  const o=+oil.value,f=+fx.value,r=+reserves.value;
  document.querySelector('#oilOut').value=`$${o}/bbl`;
  document.querySelector('#fxOut').value=fmt(f);
  document.querySelector('#reservesOut').value=`${r.toFixed(1)} months`;
  const score=Math.max(0,Math.min(100,Math.round((o-75)*.95+(f-3500)*.06+(5-r)*17)));
  let title='Contained pressure',text='The combination is consistent with manageable market pressure and an adequate external buffer.';
  if(score>=75){title='Systemic stress risk';text='Oil, FX and reserve settings reinforce each other. Escalate portfolio hedging and verify intervention capacity.'}
  else if(score>=55){title='Acute liquidity stress';text='High oil prices and a weak shilling point to near-term dollar scarcity. Confirm whether pressure persists as energy costs normalize.'}
  else if(score>=35){title='Elevated watch';text='Pressure is visible but not self-reinforcing. Maintain monitoring and avoid attributing it to a full balance-of-payments event.'}
  document.querySelector('#regime').textContent=title;document.querySelector('#regimeText').textContent=text;
  document.querySelector('#scoreBar').style.width=`${score}%`;document.querySelector('#scoreText').textContent=`PRESSURE SCORE · ${score} / 100`;
}
[oil,fx,reserves].forEach(el=>el.addEventListener('input',updateScenario));updateScenario();
document.querySelectorAll('.check input').forEach((el,i)=>{el.checked=localStorage.getItem(`oakstone-fx-action-${i}`)==='true';el.addEventListener('change',()=>localStorage.setItem(`oakstone-fx-action-${i}`,el.checked))});
document.querySelector('#themeToggle').addEventListener('click',()=>{document.body.classList.toggle('dark');localStorage.setItem('oakstone-fx-theme',document.body.classList.contains('dark')?'dark':'light')});
if(localStorage.getItem('oakstone-fx-theme')==='dark')document.body.classList.add('dark');
