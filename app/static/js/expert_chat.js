(() => {
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

  async function postChat(messagesPayload) {
    const r = await fetch('/api/expert-chat', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: messagesPayload }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) {
      const msg = d && (d.error || d.detail) ? (d.error || d.detail) : `Request failed (${r.status})`;
      throw new Error(msg);
    }
    return d;
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
    addBubble('assistant', 'Analyzing your data…');
    const thinkingNode = els.messages.lastChild;

    try {
      const payload = apiMessageList(history);
      const resp = await postChat(payload);
      const answer = resp && resp.answer ? String(resp.answer) : 'Sorry — I could not generate an answer.';

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
