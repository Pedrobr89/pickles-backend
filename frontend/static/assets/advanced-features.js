// advanced-features.js

async function fetchScoringRanking(cnpj, filtros, limite){
  try{
    const payload = { cnpj: String(cnpj||'').replace(/\D/g,''), filtros: filtros||{}, limite: Number(limite||50) };
    const r = await callApi('/api/scoring/ranking', { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(payload) });
    let d = r.data; if(typeof d==='string'){ try{ d = JSON.parse(d) }catch{} }
    return { ok: r.ok, data: d };
  }catch(e){ return { ok:false, data:{ erro: String(e&&e.message||e) } } }
}

async function fetchScoringBatch(cnpj, editais){
  try{
    const payload = { cnpj: String(cnpj||'').replace(/\D/g,''), editais: Array.isArray(editais)? editais : [] };
    const r = await callApi('/api/scoring/batch', { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(payload) });
    let d = r.data; if(typeof d==='string'){ try{ d = JSON.parse(d) }catch{} }
    return { ok: r.ok, data: d };
  }catch(e){ return { ok:false, data:{ erro: String(e&&e.message||e) } } }
}

function exportRelatorioPDF(titulo, conteudoHtml){
  try{
    const style = `<style>body{font-family:system-ui;background:#0f172a;color:#e2e8f0} h1{font-size:20px;margin:0 0 8px} .card{padding:12px;border:1px solid #334155;border-radius:12px;background:#0b1324}</style>`;
    const html = `<!doctype html><html><head><meta charset="utf-8">${style}<title>${escapeHtml(titulo||'Relatório')}</title></head><body><h1>${escapeHtml(titulo||'Relatório')}</h1>${conteudoHtml||''}</body></html>`;
    const w = window.open('about:blank', '_blank', 'noopener');
    if (w && w.document){ w.document.open(); w.document.write(html); w.document.close(); w.focus(); setTimeout(function(){ try{ w.print() }catch{} }, 300) }
    else { const url = 'data:text/html;charset=utf-8,' + encodeURIComponent(html); const a = document.createElement('a'); a.href=url; a.target='_blank'; a.rel='noopener'; a.download='relatorio.html'; document.body.appendChild(a); a.click(); a.remove() }
  }catch{}
}

window.AdvancedFeatures = { fetchScoringRanking, fetchScoringBatch, exportRelatorioPDF };

