(() => {
  /* ── FAB / Drawer toggle ── */
  const fab     = document.getElementById('mexpert-fab');
  const drawer  = document.getElementById('mexpert-drawer');
  const overlay = document.getElementById('mexpert-overlay');
  const closeBtn = document.getElementById('mexpert-close');

  function openDrawer() {
    if (!drawer) return;
    drawer.hidden = false;
    if (overlay) { overlay.hidden = false; }
    // Focus textarea after animation
    setTimeout(() => {
      const t = document.getElementById('expert-text');
      if (t) t.focus();
    }, 280);
  }

  function closeDrawer() {
    if (!drawer) return;
    drawer.classList.add('mexpert-closing');
    if (overlay) overlay.hidden = true;
    setTimeout(() => {
      drawer.hidden = true;
      drawer.classList.remove('mexpert-closing');
    }, 200);
  }

  if (fab) fab.addEventListener('click', () => {
    if (!drawer) return;
    drawer.hidden ? openDrawer() : closeDrawer();
  });
  if (closeBtn) closeBtn.addEventListener('click', closeDrawer);
  if (overlay)  overlay.addEventListener('click', closeDrawer);
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && drawer && !drawer.hidden) closeDrawer();
  });

  /* ── Chat core ── */
  const els = {
    form: document.getElementById('expert-form'),
    text: document.getElementById('expert-text'),
    send: document.getElementById('expert-send'),
    clear: document.getElementById('expert-clear'),
    messages: document.getElementById('expert-messages'),
  };

  if (!els.form || !els.text || !els.send || !els.clear || !els.messages) return;

  /** In-memory only: each full page load starts a fresh chat (no sessionStorage). */
  let history = [];

  function nowIso() {
    try { return new Date().toISOString(); } catch (_) { return null; }
  }

  function scrollToBottom() {
    try { els.messages.scrollTop = els.messages.scrollHeight; } catch (_) {}
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /** Minimal safe markdown: **bold**, `code`, paragraphs, simple bullet lines. */
  function lightMarkdownToHtml(src) {
    const raw = String(src || '');
    const blocks = raw.split(/\n{2,}/);
    const parts = [];
    for (let b = 0; b < blocks.length; b++) {
      const block = blocks[b];
      const lines = block.split('\n');
      const isList = lines.every((ln) => ln.trim() === '' || /^\s*[-*]\s+/.test(ln));
      if (isList && lines.some((ln) => ln.trim() !== '')) {
        parts.push('<ul>');
        lines.forEach((ln) => {
          const m = ln.match(/^\s*[-*]\s+(.+)$/);
          if (m) parts.push(`<li>${inlineMd(escapeHtml(m[1]))}</li>`);
        });
        parts.push('</ul>');
      } else {
        const inner = lines
          .map((ln) => (ln.trim() === '' ? '<br>' : inlineMd(escapeHtml(ln))))
          .join('<br>');
        parts.push(`<p>${inner}</p>`);
      }
    }
    return parts.join('');
  }

  function inlineMd(escapedLine) {
    let t = escapedLine;
    t = t.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    t = t.replace(/`([^`]+)`/g, '<code>$1</code>');
    return t;
  }

  function setBubbleContent(bubble, role, text) {
    if (role === 'assistant') bubble.innerHTML = lightMarkdownToHtml(text);
    else bubble.textContent = String(text || '');
  }

  function setAssistantStatusLine(bubble, text) {
    let span = bubble.querySelector('.expert-status-line');
    if (!span) {
      bubble.innerHTML = '';
      span = document.createElement('span');
      span.className = 'expert-status-line';
      bubble.appendChild(span);
    }
    span.textContent = String(text || '');
  }

  function addBubble(role, text) {
    const wrap = document.createElement('div');
    wrap.className = `expert-message ${role}`;
    const bubble = document.createElement('div');
    bubble.className = 'bubble';
    setBubbleContent(bubble, role, text);
    wrap.appendChild(bubble);
    els.messages.appendChild(wrap);
    scrollToBottom();
  }

  function renderFromHistory(hist) {
    while (els.messages.children.length > 1) els.messages.removeChild(els.messages.lastChild);
    (hist || []).forEach((m) => {
      if (!m || !m.role) return;
      if (m.role === 'system') addBubble('system', m.text || '');
      else addBubble(m.role, m.text || '');
    });
  }

  function apiMessageList(hist) {
    return (hist || [])
      .filter((m) => m && (m.role === 'user' || m.role === 'assistant') && (m.text || '').trim())
      .map((m) => ({ role: m.role, text: (m.text || '').trim() }));
  }

  function consumeSseEventBlock(rawBlock, onEvent, state) {
    const lines = rawBlock.split(/\r?\n/);
    for (const line of lines) {
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (!payload) continue;
      let ev;
      try {
        ev = JSON.parse(payload);
      } catch (_) {
        continue;
      }
      if (onEvent) onEvent(ev);
      if (ev && ev.type === 'answer') state.finalAnswer = String(ev.text || '');
      if (ev && ev.type === 'error') throw new Error(String(ev.text || 'Error'));
    }
  }

  /**
   * SSE from POST /api/expert-chat: events `data: {"type":"status"|"answer"|"error",...}\n\n`
   */
  async function postChatStream(messagesPayload, onEvent) {
    const r = await fetch('/api/expert-chat', {
      method: 'POST',
      credentials: 'include',
      cache: 'no-store',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'text/event-stream',
      },
      body: JSON.stringify({ messages: messagesPayload }),
    });
    const ct = (r.headers.get('content-type') || '').toLowerCase();
    if (!r.ok) {
      let msg = `Request failed (${r.status})`;
      if (ct.includes('application/json')) {
        const d = await r.json().catch(() => ({}));
        msg = (d && (d.error || d.detail)) ? (d.error || d.detail) : msg;
      } else {
        const t = await r.text().catch(() => '');
        if (t) msg = t.slice(0, 500);
      }
      throw new Error(msg);
    }
    if (!r.body || typeof r.body.getReader !== 'function') {
      throw new Error('Streaming not supported in this browser.');
    }
    const reader = r.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    const state = { finalAnswer: null };
    while (true) {
      const { done, value } = await reader.read();
      if (value) buf += dec.decode(value, { stream: true });
      let sep;
      while ((sep = buf.indexOf('\n\n')) !== -1) {
        const rawBlock = buf.slice(0, sep);
        buf = buf.slice(sep + 2);
        consumeSseEventBlock(rawBlock, onEvent, state);
      }
      if (done) break;
    }
    dec.decode();
    if (buf.trim()) {
      if (buf.includes('\n\n')) {
        for (const rawBlock of buf.split(/\n\n/)) {
          if (rawBlock.trim()) consumeSseEventBlock(rawBlock, onEvent, state);
        }
      } else {
        consumeSseEventBlock(buf, onEvent, state);
      }
    }
    return state.finalAnswer != null ? state.finalAnswer : 'Sorry — I could not generate an answer.';
  }

  function setBusy(busy) {
    els.send.disabled = !!busy;
    els.text.disabled = !!busy;
    if (!busy) els.text.focus();
  }

  try {
    sessionStorage.removeItem('me_expert_chat_v1');
  } catch (_) {}

  els.clear.addEventListener('click', () => {
    history = [];
    renderFromHistory([]);
    addBubble('system', 'Chat cleared.');
  });

  els.form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = (els.text.value || '').trim();
    if (!msg) return;
    els.text.value = '';

    history.push({ role: 'user', text: msg, ts: nowIso() });
    addBubble('user', msg);

    setBusy(true);
    const wrap = document.createElement('div');
    wrap.className = 'expert-message assistant';
    const bubble = document.createElement('div');
    bubble.className = 'bubble';
    setAssistantStatusLine(bubble, 'Analyzing your data…');
    wrap.appendChild(bubble);
    els.messages.appendChild(wrap);
    scrollToBottom();
    const thinkingNode = wrap;

    try {
      const payload = apiMessageList(history);
      const answer = await postChatStream(payload, (ev) => {
        if (ev && ev.type === 'status' && ev.text) {
          setAssistantStatusLine(bubble, ev.text);
          scrollToBottom();
        }
      });

      if (thinkingNode && thinkingNode.querySelector) {
        const b = thinkingNode.querySelector('.bubble');
        if (b) setBubbleContent(b, 'assistant', answer);
        thinkingNode.className = 'expert-message assistant';
      } else {
        addBubble('assistant', answer);
      }

      history.push({ role: 'assistant', text: answer, ts: nowIso() });
    } catch (err) {
      const em = (err && err.message) ? err.message : 'Network error';
      if (thinkingNode && thinkingNode.querySelector) {
        const b = thinkingNode.querySelector('.bubble');
        if (b) b.textContent = `Error: ${em}`;
        thinkingNode.className = 'expert-message system';
      } else {
        addBubble('system', `Error: ${em}`);
      }
    } finally {
      setBusy(false);
    }
  });
})();
