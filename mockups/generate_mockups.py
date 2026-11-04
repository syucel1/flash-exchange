import random, math, sys, html
SEED=int(__import__('os').environ.get('SEED','29'))
random.seed(SEED)

# ---------------------------------------------------------------- data
N = 96
LAST = 1.27184
def walk(n, end, vol):
    xs=[end]
    for _ in range(n-1): xs.append(xs[-1] - random.gauss(0.000025, vol))
    return xs[::-1]
closes = walk(N, LAST, 0.00038)
candles=[]; prev=closes[0]-0.0002
for i,c in enumerate(closes):
    o = prev; hi=max(o,c)+abs(random.gauss(0,0.00018)); lo=min(o,c)-abs(random.gauss(0,0.00018))
    v = abs(random.gauss(1.0,0.45))*(1.6 if abs(c-o)>0.0004 else 1)
    candles.append((o,hi,lo,c,v)); prev=c
OPEN24 = candles[0][0]
CHG = LAST-OPEN24
HI24 = max(c[1] for c in candles); LO24=min(c[2] for c in candles)

bids=[1.27180,1.27176,1.27170,1.27165,1.27160,1.27155,1.27150,1.27145,1.27140,1.27135,1.27130,1.27120]
asks=[1.27186,1.27190,1.27195,1.27200,1.27205,1.27210,1.27218,1.27225,1.27232,1.27240,1.27248,1.27255]
def sizes(n):
    return [random.choice([1,2,2,3,4,5,6,8])*random.choice([25_000,50_000,100_000]) + random.randint(0,9)*5_000 for _ in range(n)]
bsz=sizes(12); asz=sizes(12); bcnt=[random.randint(1,9) for _ in bids]; acnt=[random.randint(1,9) for _ in asks]
OWN_BID={1.27150}; OWN_ASK={1.27240}

def fp(p): return f"{p:.5f}"
def fq(q): return f"{q:,.0f}"

# ---------------------------------------------------------------- themes
THEMES = {
 'light': dict(bg='#f3f5f9', panel='#ffffff', panel2='#f7f8fb', text='#0f172a', muted='#64748b', faint='#94a3b8',
               line='#e6e9ef', grid='#eef1f5', up='#0e9f6e', down='#e11d48', upbg='rgba(14,159,110,.13)',
               downbg='rgba(225,29,72,.12)', accent='#5b5bf6', accent2='#06b6d4', chip='#eef0f6', input='#f4f6fa',
               own='#f59e0b', vol_up='rgba(14,159,110,.35)', vol_down='rgba(225,29,72,.32)', ma1='#5b5bf6', ma2='#f59e0b', bb='rgba(91,91,246,.10)'),
 'dark':  dict(bg='#0a0e17', panel='#111723', panel2='#0d121c', text='#e6ebf3', muted='#8b97ab', faint='#5f6b80',
               line='#1d2533', grid='#182030', up='#22c38e', down='#f6465d', upbg='rgba(34,195,142,.14)',
               downbg='rgba(246,70,93,.14)', accent='#818cf8', accent2='#22d3ee', chip='#1a2130', input='#161d2a',
               own='#fbbf24', vol_up='rgba(34,195,142,.35)', vol_down='rgba(246,70,93,.35)', ma1='#818cf8', ma2='#fbbf24', bb='rgba(129,140,248,.10)'),
}

# ---------------------------------------------------------------- svg charts
def price_axis(y0,y1,h,top,bottom,x,w,T,n=6):
    out=[]; 
    for i in range(n+1):
        p = bottom + (top-bottom)*i/n; y = y1 - (p-bottom)/(top-bottom)*(y1-y0)
        out.append(f'<line x1="0" x2="{x}" y1="{y:.1f}" y2="{y:.1f}" stroke="{T["grid"]}"/>')
        out.append(f'<text x="{x+8}" y="{y+4:.1f}" fill="{T["muted"]}" font-size="11">{fp(p)}</text>')
    return ''.join(out)

def time_axis(x0,x1,y,count,T,stride=12):
    out=[]; step=(x1-x0)/count
    for i in range(stride//2,count,stride):
        hh=(i*15//60 + 16)%24; mm=(i*15)%60
        x=x0+step*(i+0.5)
        out.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="0" y2="{y}" stroke="{T["grid"]}"/>')
        out.append(f'<text x="{x:.1f}" y="{y+16}" fill="{T["muted"]}" font-size="11" text-anchor="middle">{hh:02d}:{mm:02d}</text>')
    return ''.join(out)

def sma(vals,n): return [None if i<n-1 else sum(vals[i-n+1:i+1])/n for i in range(len(vals))]

def candle_svg(W,H,T,data,show_ma=True,show_bb=False,vol_frac=0.18,rsi=False):
    axisw=78; plotw=W-axisw; xh=24
    rsi_h = 90 if rsi else 0
    ph = H - xh - rsi_h
    vol_h = ph*vol_frac; price_h = ph - vol_h - 8
    lo=min(c[2] for c in data); hi=max(c[1] for c in data); pad=(hi-lo)*0.08; lo-=pad; hi+=pad
    def Y(p): return 8 + (hi-p)/(hi-lo)*(price_h-8)
    n=len(data); step=plotw/n; bw=max(2,step*0.62)
    s=[f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" font-family="Inter, sans-serif">']
    s.append(price_axis(8,price_h,0,hi,lo,plotw,axisw,T))
    s.append(time_axis(0,plotw,H-xh- rsi_h if False else ph,n,T))
    closes=[c[3] for c in data]
    if show_bb:
        m=sma(closes,20); up=[];dn=[]
        for i in range(n):
            if m[i] is None: continue
            sd=math.sqrt(sum((closes[j]-m[i])**2 for j in range(i-19,i+1))/20)
            up.append((step*(i+.5),Y(m[i]+2*sd))); dn.append((step*(i+.5),Y(m[i]-2*sd)))
        pts=' '.join(f'{x:.1f},{y:.1f}' for x,y in up+dn[::-1])
        s.append(f'<polygon points="{pts}" fill="{T["bb"]}"/>')
    vmax=max(c[4] for c in data)
    for i,(o,h,l,c,v) in enumerate(data):
        x=step*(i+.5); col=T['up'] if c>=o else T['down']
        s.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{Y(h):.1f}" y2="{Y(l):.1f}" stroke="{col}" stroke-width="1"/>')
        y1=Y(max(o,c)); y2=Y(min(o,c)); 
        s.append(f'<rect x="{x-bw/2:.1f}" y="{y1:.1f}" width="{bw:.1f}" height="{max(1,y2-y1):.1f}" fill="{col}" rx="0.5"/>')
        vh=v/vmax*(vol_h-6)
        s.append(f'<rect x="{x-bw/2:.1f}" y="{ph-vh:.1f}" width="{bw:.1f}" height="{vh:.1f}" fill="{T["vol_up"] if c>=o else T["vol_down"]}"/>')
    if show_ma:
        for per,col in ((9,T['ma1']),(30,T['ma2'])):
            m=sma(closes,per); pts=' '.join(f'{step*(i+.5):.1f},{Y(v):.1f}' for i,v in enumerate(m) if v is not None)
            s.append(f'<polyline points="{pts}" fill="none" stroke="{col}" stroke-width="1.6"/>')
    # last price line + label
    yl=Y(closes[-1]); col=T['up'] if CHG>=0 else T['down']
    s.append(f'<line x1="0" x2="{plotw}" y1="{yl:.1f}" y2="{yl:.1f}" stroke="{col}" stroke-dasharray="3 3"/>')
    s.append(f'<rect x="{plotw+2}" y="{yl-10:.1f}" width="{axisw-6}" height="20" rx="4" fill="{col}"/><text x="{plotw+8}" y="{yl+4:.1f}" fill="#fff" font-size="11" font-weight="600">{fp(closes[-1])}</text>')
    # crosshair
    ci=int(n*0.72); cx=step*(ci+.5); cy=Y(data[ci][3])
    s.append(f'<line x1="{cx:.1f}" x2="{cx:.1f}" y1="0" y2="{ph}" stroke="{T["faint"]}" stroke-dasharray="4 4"/><line x1="0" x2="{plotw}" y1="{cy:.1f}" y2="{cy:.1f}" stroke="{T["faint"]}" stroke-dasharray="4 4"/>')
    s.append(f'<rect x="{plotw+2}" y="{cy-10:.1f}" width="{axisw-6}" height="20" rx="4" fill="{T["chip"]}" stroke="{T["line"]}"/><text x="{plotw+8}" y="{cy+4:.1f}" fill="{T["text"]}" font-size="11">{fp(data[ci][3])}</text>')
    o,h,l,c,v=data[ci]
    s.append(f'<text x="10" y="22" font-size="11.5" fill="{T["muted"]}">O <tspan fill="{T["text"]}">{fp(o)}</tspan>  H <tspan fill="{T["text"]}">{fp(h)}</tspan>  L <tspan fill="{T["text"]}">{fp(l)}</tspan>  C <tspan fill="{T["text"]}">{fp(c)}</tspan>  Vol <tspan fill="{T["text"]}">{v*42:.1f}M</tspan></text>')
    if show_ma:
        s.append(f'<text x="10" y="40" font-size="11.5" fill="{T["ma1"]}">MA 9</text><text x="56" y="40" font-size="11.5" fill="{T["ma2"]}">MA 30</text>' + (f'<text x="112" y="40" font-size="11.5" fill="{T["muted"]}">BB 20,2</text>' if show_bb else ''))
    if rsi:
        top=ph+xh+4; bot=H-6
        s.append(f'<line x1="0" x2="{W}" y1="{top-4}" y2="{top-4}" stroke="{T["line"]}"/>')
        gains=[max(0,closes[i]-closes[i-1]) for i in range(1,n)]; losses=[max(0,closes[i-1]-closes[i]) for i in range(1,n)]
        vals=[]
        for i in range(14,n):
            g=sum(gains[i-14:i])/14; lo_=sum(losses[i-14:i])/14 or 1e-9; vals.append((i,100-100/(1+g/lo_)))
        def RY(v): return top + (100-v)/100*(bot-top)
        for lvl in (30,70):
            s.append(f'<line x1="0" x2="{plotw}" y1="{RY(lvl):.1f}" y2="{RY(lvl):.1f}" stroke="{T["faint"]}" stroke-dasharray="3 4"/><text x="{plotw+8}" y="{RY(lvl)+4:.1f}" font-size="11" fill="{T["muted"]}">{lvl}</text>')
        pts=' '.join(f'{step*(i+.5):.1f},{RY(v):.1f}' for i,v in vals)
        s.append(f'<polyline points="{pts}" fill="none" stroke="{T["accent2"]}" stroke-width="1.5"/><text x="10" y="{top+14}" font-size="11.5" fill="{T["accent2"]}">RSI 14  {vals[-1][1]:.1f}</text>')
    s.append('</svg>'); return ''.join(s)

def area_svg(W,H,T,data):
    axisw=78; plotw=W-axisw; ph=H-24
    closes=[c[3] for c in data]; lo=min(closes); hi=max(closes); pad=(hi-lo)*.15; lo-=pad; hi+=pad
    def Y(p): return 8+(hi-p)/(hi-lo)*(ph-16)
    n=len(closes); step=plotw/(n-1)
    s=[f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" font-family="Inter, sans-serif"><defs><linearGradient id="ag" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="{T["accent"]}" stop-opacity=".35"/><stop offset="1" stop-color="{T["accent"]}" stop-opacity="0"/></linearGradient></defs>']
    s.append(price_axis(8,ph-8,0,hi,lo,plotw,axisw,T))
    s.append(time_axis(-step/2,plotw+step/2,ph,n,T))
    pts=[(i*step,Y(c)) for i,c in enumerate(closes)]
    line=' '.join(f'{x:.1f},{y:.1f}' for x,y in pts)
    s.append(f'<polygon points="0,{ph} {line} {plotw},{ph}" fill="url(#ag)"/><polyline points="{line}" fill="none" stroke="{T["accent"]}" stroke-width="2"/>')
    yl=Y(closes[-1])
    s.append(f'<circle cx="{plotw:.1f}" cy="{yl:.1f}" r="4" fill="{T["accent"]}"/><circle cx="{plotw:.1f}" cy="{yl:.1f}" r="9" fill="{T["accent"]}" opacity=".25"/>')
    s.append(f'<rect x="{plotw+2}" y="{yl-10:.1f}" width="{axisw-6}" height="20" rx="4" fill="{T["accent"]}"/><text x="{plotw+8}" y="{yl+4:.1f}" fill="#fff" font-size="11" font-weight="600">{fp(closes[-1])}</text>')
    s.append(f'<text x="10" y="22" font-size="11.5" fill="{T["muted"]}">GBP/USD · 15m · Area</text>')
    s.append('</svg>'); return ''.join(s)

def depth_svg(W,H,T):
    ph=H-26; mid=(bids[0]+asks[0])/2
    lo=bids[-1]-0.00005; hi=asks[-1]+0.00005
    def X(p): return (p-lo)/(hi-lo)*W
    cb=[];acc=0
    for p,q in zip(bids,bsz): acc+=q; cb.append((p,acc))
    ca=[];acc=0
    for p,q in zip(asks,asz): acc+=q; ca.append((p,acc))
    mx=max(cb[-1][1],ca[-1][1])*1.1
    def Y(q): return 10+(1-q/mx)*(ph-10)
    s=[f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" font-family="Inter, sans-serif">']
    for i in range(5):
        y=10+(ph-10)*i/4; s.append(f'<line x1="0" x2="{W}" y1="{y:.1f}" y2="{y:.1f}" stroke="{T["grid"]}"/>')
        s.append(f'<text x="6" y="{y-3:.1f}" fill="{T["muted"]}" font-size="10.5">{mx*(1-i/4)/1e6:.1f}M</text>')
    # bids step from mid leftwards
    pts=[(X(mid),Y(0))]; prevq=0
    for p,q in cb: pts+= [(X(p),Y(prevq)),(X(p),Y(q))]; prevq=q
    pts.append((0,Y(prevq))); pts.append((0,Y(0)))
    s.append(f'<polygon points="{" ".join(f"{x:.1f},{y:.1f}" for x,y in pts)}" fill="{T["upbg"]}" stroke="{T["up"]}" stroke-width="1.6"/>')
    pts=[(X(mid),Y(0))]; prevq=0
    for p,q in ca: pts+= [(X(p),Y(prevq)),(X(p),Y(q))]; prevq=q
    pts.append((W,Y(prevq))); pts.append((W,Y(0)))
    s.append(f'<polygon points="{" ".join(f"{x:.1f},{y:.1f}" for x,y in pts)}" fill="{T["downbg"]}" stroke="{T["down"]}" stroke-width="1.6"/>')
    s.append(f'<line x1="{X(mid):.1f}" x2="{X(mid):.1f}" y1="0" y2="{ph}" stroke="{T["faint"]}" stroke-dasharray="3 3"/>')
    s.append(f'<text x="{X(mid):.1f}" y="18" text-anchor="middle" font-size="11.5" fill="{T["text"]}" font-weight="600">Mid {mid:.5f}</text>')
    for p in (bids[-1],bids[5],mid,asks[5],asks[-1]):
        s.append(f'<text x="{X(p):.1f}" y="{H-8}" text-anchor="middle" font-size="10.5" fill="{T["muted"]}">{fp(p)}</text>')
    s.append('</svg>'); return ''.join(s)

# ---------------------------------------------------------------- html pieces
ICON = '<svg width="30" height="30" viewBox="0 0 32 32"><defs><linearGradient id="lg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#6366f1"/><stop offset="1" stop-color="#06b6d4"/></linearGradient></defs><rect width="32" height="32" rx="8" fill="url(#lg)"/><path d="M18 5 9 18h6l-2 9 10-14h-6l1-8z" fill="#fff"/></svg>'

def header(T, active='Trade'):
    nav=''.join(f'<a class="{"on" if n==active else ""}">{n}</a>' for n in ['Trade','Markets','Portfolio','Funding','History'])
    moon = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>'
    return f'''<header><div class="brand">{ICON}<span>Flash<b>Ex</b></span></div><nav>{nav}</nav>
    <div class="hr"><span class="live"><i></i>Live</span><span class="bal">100,000.00 USD</span><span class="ico">{moon}</span><span class="av">ST</span></div></header>'''

def ticker(T):
    col='up' if CHG>=0 else 'down'
    return f'''<section class="ticker panel"><div class="pair">GBP/USD <small>▾</small></div><div class="px {col}">{fp(LAST)}</div><div class="chg {col}">{CHG/OPEN24*100:+.2f}%</div>
    <div class="st"><span>24h change</span><b class="{col}">{CHG:+.5f}</b></div><div class="st"><span>24h high</span><b>{fp(HI24)}</b></div>
    <div class="st"><span>24h low</span><b>{fp(LO24)}</b></div><div class="st"><span>24h vol (GBP)</span><b>182.43M</b></div><div class="st"><span>24h vol (USD)</span><b>231.95M</b></div>
    <div class="st"><span>Spread</span><b>0.6 pips</b></div><div class="grow"></div><div class="st right"><span>Session</span><b>24/7 · Open</b></div></section>'''

def chart_toolbar(kind):
    tfs=''.join(f'<span class="{"on" if t=="15m" else ""}">{t}</span>' for t in ['1m','5m','15m','1h','4h','1D'])
    kinds=''.join(f'<span class="{"on" if k==kind else ""}">{k}</span>' for k in ['Candles','Line','Area','Depth'])
    return f'<div class="seg">{tfs}</div><div class="seg">{kinds}</div>'

def orderbook(T, rows=12):
    mb=max(bsz+asz); r=[]
    for i in range(rows):
        bown = '<i class="own"></i>' if bids[i] in OWN_BID else ''
        aown = '<i class="own"></i>' if asks[i] in OWN_ASK else ''
        r.append(f'''<div class="obr"><span class="c">{bcnt[i]}</span><span class="n">{fq(bsz[i])}</span>
        <span class="bid {"mine" if bown else ""}" style="--w:{bsz[i]/mb*100:.0f}%">{bown}{fp(bids[i])}</span>
        <span class="ask {"mine" if aown else ""}" style="--w:{asz[i]/mb*100:.0f}%">{fp(asks[i])}{aown}</span>
        <span class="n">{fq(asz[i])}</span><span class="c r">{acnt[i]}</span></div>''')
    return f'''<section class="panel ob"><div class="ph"><b>Order book</b><div class="sel">0.00001 ▾</div></div>
    <div class="spread">Spread <b>0.00006</b><span>0.6 pips · Mid 1.27183</span></div>
    <div class="obh"><span>#</span><span class="n">SIZE (GBP)</span><span class="r">BID</span><span>ASK</span><span class="n">SIZE (GBP)</span><span class="r">#</span></div>
    {''.join(r)}<div class="legend"><i class="own"></i> Your orders at this level</div></section>'''

def order_entry():
    return '''<section class="panel oe"><div class="ph"><b>Place order</b></div>
    <div class="bs"><span class="buy on">Buy</span><span>Sell</span></div>
    <div class="tabs2"><span class="on">Limit</span><span>Market</span><span>Stop</span></div>
    <div class="kv"><span>Available</span><b>100,000.00 USD <a>Deposit</a></b></div>
    <div class="inp"><span>Price</span><b>1.27150</b><em>USD</em></div>
    <div class="inp"><span>Quantity</span><b>50,000</b><em>GBP</em></div>
    <div class="pct"><span>25%</span><span class="on">50%</span><span>75%</span><span>100%</span></div>
    <div class="selx">Good till cancelled (GTC) <span>▾</span></div>
    <label class="chk"><i></i>Post-only</label>
    <div class="sum"><div><span>Value</span><b>63,575.00 USD</b></div><div><span>Est. fee (0.01%)</span><b>6.36 USD</b></div><div><span>Total cost</span><b>63,581.36 USD</b></div></div>
    <div class="cta">Buy GBP</div></section>'''

def recent_trades(n=12):
    random.seed(11); r=[]; p=LAST
    for i in range(n):
        side=random.random()<0.55; p=round(p+random.choice([-1,1])*random.choice([0,0.00002,0.00004]),5)
        r.append(f'<div class="rt"><span class="{"up" if side else "down"}">{fp(p)}</span><span>{random.choice([10,25,50,75,100,150,250])*1000:,}</span><span class="m">16:{27-i//3:02d}:{59-(i*7)%60:02d}</span></div>')
    return f'<section class="panel trades"><div class="ph"><b>Recent trades</b></div><div class="rt h"><span>PRICE (USD)</span><span>SIZE (GBP)</span><span>TIME</span></div>{"".join(r)}</section>'

def blotter():
    rows=[('16:21:04','GBP/USD','BUY','Limit · GTC','1.27150','50,000','20,000','30,000','1.27150','Partially filled',40),
          ('16:18:47','GBP/USD','SELL','Limit · GTC','1.27240','100,000','0','100,000','—','Open',0),
          ('15:52:10','GBP/USD','BUY','Stop-limit · GTC','1.27050','25,000','0','25,000','—','Pending trigger',0),
          ('15:40:22','GBP/USD','SELL','Take-profit · OCO','1.27400','25,000','0','25,000','—','Pending trigger',0),
          ('15:31:09','GBP/USD','BUY','Iceberg · GTC','1.27100','500,000','120,000','380,000','1.27100','Partially filled',24)]
    r=''.join(f'''<div class="br"><span>{a}</span><span>{b}</span><span class="{"up" if c=="BUY" else "down"}">{c}</span><span>{d}</span><span>{e}</span><span>{f}</span>
    <span><div class="bar"><i style="width:{pc}%"></i></div>{g}</span><span>{h}</span><span>{i}</span><span><em class="st {"pf" if pc else ""}">{j}</em></span><span class="x">✕</span></div>''' for a,b,c,d,e,f,g,h,i,j,pc in rows)
    return f'''<section class="panel blot" style="height:100%"><div class="ph tabs"><span class="on">Open orders <em>5</em></span><span>Order history</span><span>Trades</span><span>Portfolio</span>
    <div class="grow"></div><label class="chk2"><i></i>GBP/USD only</label><span class="ca">Cancel all</span></div>
    <div class="br h"><span>TIME</span><span>PAIR</span><span>SIDE</span><span>TYPE</span><span>PRICE</span><span>QUANTITY</span><span>FILLED</span><span>REMAINING</span><span>AVG PRICE</span><span>STATUS</span><span></span></div>{r}</section>'''

CSS = '''
*{box-sizing:border-box;margin:0}body{background:var(--bg);color:var(--text);font:13px/1.4 Inter,system-ui,sans-serif;width:1600px;height:1000px;overflow:hidden;font-feature-settings:"tnum"}
header{height:58px;display:flex;align-items:center;gap:26px;padding:0 20px;background:var(--panel);border-bottom:1px solid var(--line)}
.brand{display:flex;align-items:center;gap:9px;font-size:19px;font-weight:700;letter-spacing:-.02em}.brand b{background:linear-gradient(90deg,#6366f1,#06b6d4);-webkit-background-clip:text;color:transparent}
nav{display:flex;gap:4px}nav a{padding:7px 14px;border-radius:8px;color:var(--muted);font-weight:500;font-size:14px}nav a.on{background:var(--chip);color:var(--text)}
.hr{margin-left:auto;display:flex;align-items:center;gap:14px}.live{display:flex;align-items:center;gap:6px;padding:4px 10px;border:1px solid var(--line);border-radius:99px;color:var(--up);font-weight:600;font-size:12px}.live i{width:7px;height:7px;border-radius:50%;background:var(--up);box-shadow:0 0 0 3px var(--upbg)}
.bal{color:var(--muted);font-size:12.5px}.ico{color:var(--muted);display:flex}.av{width:32px;height:32px;border-radius:50%;background:linear-gradient(135deg,#6366f1,#06b6d4);color:#fff;display:grid;place-items:center;font-weight:700;font-size:12px}
main{padding:10px;display:grid;gap:10px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden}
.ph{display:flex;align-items:center;justify-content:space-between;padding:10px 14px;border-bottom:1px solid var(--line);font-size:13.5px;min-height:44px}
.ticker{display:flex;align-items:center;gap:26px;padding:10px 18px;height:62px}.pair{font-size:19px;font-weight:700}.pair small{color:var(--muted);font-size:12px}.px{font-size:22px;font-weight:700;letter-spacing:-.01em}.chg{font-weight:600}
.st span{display:block;color:var(--muted);font-size:11.5px}.st b{font-weight:600;font-size:13px}.grow{flex:1}
.up{color:var(--up)}.down{color:var(--down)}
.seg{display:flex;background:var(--chip);border-radius:8px;padding:3px;gap:2px}.seg span{padding:4px 9px;border-radius:6px;color:var(--muted);font-size:12px;font-weight:500}.seg span.on{background:var(--panel);color:var(--text);box-shadow:0 1px 2px rgba(0,0,0,.12)}
.chart .tools{display:flex;gap:10px;align-items:center}
.ob .sel{padding:4px 10px;border:1px solid var(--line);border-radius:7px;color:var(--muted);font-size:12px}
.spread{display:flex;gap:6px;padding:8px 14px;color:var(--muted);font-size:12px;border-bottom:1px solid var(--line)}.spread b{color:var(--text)}.spread span{margin-left:auto}
.obh,.obr{display:grid;grid-template-columns:24px 1fr 1fr 1fr 1fr 24px;align-items:center;padding:0 10px;gap:6px}
.obh{color:var(--faint);font-size:10.5px;font-weight:600;letter-spacing:.04em;height:28px}.obh .n{text-align:right}.obh .r{text-align:right}
.obr{height:28.5px;font-size:12.5px}.obr .c{color:var(--faint);font-size:11px}.obr .r{text-align:right}.obr .n{text-align:right}
.obr .bid,.obr .ask{position:relative;padding:3px 10px;white-space:nowrap;font-weight:600;border-radius:3px;z-index:0}
.obr .bid{color:var(--up);text-align:right}.obr .ask{color:var(--down)}
.obr .bid::before,.obr .ask::before{content:"";position:absolute;top:1px;bottom:1px;width:var(--w);z-index:-1;border-radius:3px}
.obr .bid::before{right:0;background:var(--upbg)}.obr .ask::before{left:0;background:var(--downbg)}
.obr .mine{outline:1.5px solid var(--own);outline-offset:-1px}.obr .bid i.own{position:absolute;left:-2px;top:50%;margin:-3.5px 0 0}.obr .ask i.own{position:absolute;right:-2px;top:50%;margin:-3.5px 0 0}
i.own{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--own);margin:0 6px;vertical-align:1px;box-shadow:0 0 0 3px rgba(245,158,11,.22)}
.legend{padding:9px 14px;color:var(--muted);font-size:11.5px;border-top:1px solid var(--line)}
.oe{padding-bottom:12px}.oe>div{margin:10px 14px 0}.bs{display:grid;grid-template-columns:1fr 1fr;background:var(--chip);border-radius:9px;padding:3px}.bs span{text-align:center;padding:8px;border-radius:7px;font-weight:600;color:var(--muted)}.bs .buy.on{background:var(--up);color:#fff}
.tabs2{display:grid;grid-template-columns:repeat(3,1fr);border:1px solid var(--line);border-radius:9px;padding:3px}.tabs2 span{text-align:center;padding:6px;border-radius:7px;color:var(--muted);font-weight:500;font-size:12.5px}.tabs2 .on{background:var(--chip);color:var(--text)}
.kv{display:flex;justify-content:space-between;color:var(--muted);font-size:12.5px}.kv b{color:var(--text);font-weight:600}.kv a{color:var(--accent);margin-left:6px}
.inp{display:flex;align-items:center;background:var(--input);border:1px solid var(--line);border-radius:9px;padding:11px 12px;gap:8px}.inp span{color:var(--muted)}.inp b{margin-left:auto;font-size:15px}.inp em{font-style:normal;color:var(--muted);font-size:12px}
.pct{display:grid;grid-template-columns:repeat(4,1fr);gap:6px}.pct span{text-align:center;border:1px solid var(--line);border-radius:7px;padding:6px;color:var(--muted);font-size:12px}.pct .on{border-color:var(--accent);color:var(--accent)}
.selx{display:flex;justify-content:space-between;border:1px solid var(--line);border-radius:9px;padding:9px 12px;font-size:12.5px}
.chk{margin:10px 14px 0}.chk,.chk2{display:flex;align-items:center;gap:8px;color:var(--muted);font-size:12.5px}.chk i,.chk2 i{width:15px;height:15px;border:1.5px solid var(--faint);border-radius:4px}
.sum{border-top:1px solid var(--line);padding-top:10px}.sum div{display:flex;justify-content:space-between;font-size:12.5px;color:var(--muted);padding:2px 0}.sum b{color:var(--text);font-weight:600}
.cta{background:var(--up);color:#fff;text-align:center;padding:12px;border-radius:10px;font-weight:700;font-size:14.5px}
.trades .rt{display:grid;grid-template-columns:1fr 1fr 74px;padding:3.6px 14px;font-size:12.5px}.trades .rt span:nth-child(2){text-align:right}.trades .rt .m{color:var(--muted);text-align:right}.trades .rt.h span:nth-child(3){text-align:right}.trades .rt.h{color:var(--faint);font-size:10.5px;font-weight:600;padding-top:8px}
.tabs{justify-content:flex-start;gap:22px}.tabs span{color:var(--muted);font-weight:500}.tabs span.on{color:var(--text);font-weight:600;box-shadow:inset 0 -2px 0 var(--accent);padding-bottom:10px;margin-bottom:-11px}.tabs em{font-style:normal;background:var(--chip);padding:1px 7px;border-radius:99px;font-size:11px;margin-left:4px}
.ca{color:var(--down);border:1px solid var(--line);padding:4px 10px;border-radius:7px;font-size:12px}
.br{display:grid;grid-template-columns:80px 80px 60px 130px 80px 90px 120px 90px 80px 130px 30px;padding:9px 14px;font-size:12.5px;align-items:center;border-bottom:1px solid var(--line)}
.br.h{color:var(--faint);font-size:10.5px;font-weight:600;letter-spacing:.04em;padding:8px 14px}.br .x{color:var(--muted);text-align:center}
.bar{display:inline-block;width:48px;height:5px;background:var(--chip);border-radius:3px;margin-right:6px;vertical-align:2px;overflow:hidden}.bar i{display:block;height:100%;background:var(--accent)}
em.st{font-style:normal;font-size:11.5px;padding:2px 8px;border-radius:99px;background:var(--chip);color:var(--muted)}em.st.pf{background:rgba(99,102,241,.15);color:var(--accent)}
.side{display:grid;gap:10px}
.ind{display:flex;gap:6px}.ind span{padding:4px 9px;border-radius:99px;font-size:11.5px;border:1px solid var(--line);color:var(--muted)}.ind span.on{border-color:var(--accent);color:var(--accent)}
.stats{display:grid;grid-template-columns:repeat(2,1fr);gap:0}.stats div{padding:10px 14px;border-bottom:1px solid var(--line)}.stats span{display:block;color:var(--muted);font-size:11.5px}.stats b{font-size:14px}
'''

def page(theme, body):
    T=THEMES[theme]; vars=';'.join(f'--{k.replace("_","-")}:{v}' for k,v in T.items())
    return f'<!doctype html><html><head><meta charset="utf-8"><title>FlashEx mockup</title><style>:root{{{vars}}}{CSS}</style></head><body>{body}</body></html>'

def trade_screen(theme):
    T=THEMES[theme]
    chart=candle_svg(800,436,T,candles)
    body=header(T)+'<main style="grid-template-columns:830px 390px 1fr;grid-template-rows:62px 500px 1fr">'
    body+=f'<div style="grid-column:1/4">{ticker(T)}</div>'
    body+=f'<section class="panel chart" style="grid-column:1;grid-row:2"><div class="ph"><b>Chart</b><div class="tools">{chart_toolbar("Candles")}</div></div><div style="padding:4px 14px 0">{chart}</div></section>'
    body+=f'<div style="grid-column:2;grid-row:2">{orderbook(T)}</div>'
    body+=f'<div style="grid-column:3;grid-row:2/4">{order_entry()}<div style="height:10px"></div>{recent_trades(8)}</div>'
    body+=f'<div style="grid-column:1/3;grid-row:3">{blotter()}</div></main>'
    return page(theme, body)

def chart_screen(theme, kind):
    T=THEMES[theme]
    if kind=='candles':
        main_svg=candle_svg(1240,760,T,candles,show_ma=True,show_bb=True,vol_frac=.16,rsi=True)
        title='Candles'; extra=''
    else:
        main_svg=area_svg(1240,470,T,candles); title='Area'
        extra=f'<div class="ph" style="border-top:1px solid var(--line)"><b>Market depth</b><div class="seg"><span class="on">±0.1%</span><span>±0.5%</span><span>±1%</span></div></div><div style="padding:6px 14px">{depth_svg(1240,250,T)}</div>'
    ind=''.join(f'<span class="{"on" if (i in ("MA","BB","RSI") and kind=="candles") or (i=="MA" and kind!="candles") else ""}">{i}</span>' for i in ['MA','EMA','BB','VWAP','RSI','MACD'])
    stats=''.join(f'<div><span>{a}</span><b class="{c}">{b}</b></div>' for a,b,c in [
        ('Last',fp(LAST),'up'),('24h change',f'{CHG:+.5f}','up' if CHG>=0 else 'down'),('24h high',fp(HI24),''),('24h low',fp(LO24),''),
        ('Best bid',fp(bids[0]),'up'),('Best ask',fp(asks[0]),'down'),('Vol (GBP)','182.43M',''),('Trades','48,213',''),('VWAP 24h','1.27201',''),('Your open orders','5','')])
    body=header(T)+'<main style="grid-template-columns:1fr 300px;grid-template-rows:62px 1fr">'
    body+=f'<div style="grid-column:1/3">{ticker(T)}</div>'
    body+=f'<section class="panel chart" style="grid-column:1;grid-row:2"><div class="ph"><div class="tools">{chart_toolbar(title)}<div class="ind">{ind}</div></div><span style="color:var(--muted);font-size:12px">GBP/USD · FlashEx · 15m</span></div><div style="padding:6px 14px">{main_svg}</div>{extra}</section>'
    body+=f'<div class="side" style="grid-column:2;grid-row:2"><section class="panel"><div class="ph"><b>Market stats</b></div><div class="stats">{stats}</div></section>{recent_trades(16)}</div></main>'
    return page(theme, body)

import os
out=sys.argv[1]; os.makedirs(out,exist_ok=True)
for th in ('light','dark'):
    open(f'{out}/trade-{th}.html','w').write(trade_screen(th))
    open(f'{out}/chart-candles-{th}.html','w').write(chart_screen(th,'candles'))
    open(f'{out}/chart-area-{th}.html','w').write(chart_screen(th,'area'))
print('ok')
