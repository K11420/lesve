const API_URL = window.location.origin;
const token = localStorage.getItem('token');

if (!token) {
    sessionStorage.setItem('auth_redirect', window.location.pathname + window.location.search);
    window.location.href = '/auth.html';
}

const CATEGORY_LABELS = {
    bug_report: 'バグ報告',
    question: '質問',
    inquiry: 'お問い合わせ'
};

const STATUS_LABELS = {
    open: '受付中',
    waiting_admin: '運営対応待ち',
    waiting_user: 'ユーザー返信待ち',
    closed: '完了'
};
const DISPLAY_LOCALE = 'ja-JP';
const DISPLAY_TIME_ZONE = 'Asia/Tokyo';

let currentUser = null;
let tickets = [];
let activeTicketId = null;
let activeTicketDetail = null;
let requestedTicketId = null;

const query = new URLSearchParams(window.location.search);
const requestedScope = query.get('scope');
requestedTicketId = query.get('ticket');

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = String(text ?? '');
    return div.innerHTML;
}

function formatCategory(category) {
    return CATEGORY_LABELS[category] || category || '未分類';
}

function formatStatus(status) {
    return STATUS_LABELS[status] || status || '不明';
}

function statusClass(status) {
    return String(status || '').toLowerCase().replace(/[^a-z_]/g, '') || 'open';
}

function truncateText(text, max = 84) {
    const value = String(text ?? '').trim();
    if (value.length <= max) return value;
    return `${value.slice(0, Math.max(1, max - 1))}…`;
}

function formatJstDateTime(value, options = {}, fallback = '-') {
    if (value === null || value === undefined || value === '') return fallback;
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return fallback;
    return date.toLocaleString(DISPLAY_LOCALE, { timeZone: DISPLAY_TIME_ZONE, ...options });
}

async function api(endpoint, options = {}) {
    const response = await fetch(`${API_URL}${endpoint}`, {
        ...options,
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`,
            ...options.headers
        }
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        throw new Error(data.error || 'APIエラー');
    }
    return data;
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    const icons = {
        success: 'fa-check-circle',
        error: 'fa-exclamation-circle',
        warning: 'fa-exclamation-triangle',
        info: 'fa-info-circle'
    };
    toast.innerHTML = `<i class="fas ${icons[type] || icons.info}"></i><span>${escapeHtml(message)}</span>`;
    container.appendChild(toast);
    setTimeout(() => toast.classList.add('show'), 10);
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

function getCurrentScope() {
    const scopeEl = document.getElementById('chat-scope-select');
    if (!currentUser?.is_admin) return 'mine';
    return scopeEl?.value === 'all' ? 'all' : 'mine';
}

function updateScopeVisibility() {
    const scopeRow = document.getElementById('support-chat-scope-row');
    const adminStatusRow = document.getElementById('support-chat-admin-status');
    if (scopeRow) {
        scopeRow.style.display = currentUser?.is_admin ? 'grid' : 'none';
    }
    if (adminStatusRow) {
        adminStatusRow.style.display = currentUser?.is_admin ? 'flex' : 'none';
    }
}

function renderTaskbar() {
    const barEl = document.getElementById('chat-taskbar');
    const countEl = document.getElementById('chat-taskbar-count');
    if (countEl) countEl.textContent = `${tickets.length}件`;
    if (!barEl) return;

    if (!tickets.length) {
        barEl.innerHTML = '<div class="text-muted text-center" style="padding: 0.8rem;">問い合わせはまだありません</div>';
        return;
    }

    const showOwner = currentUser?.is_admin && getCurrentScope() === 'all';
    barEl.innerHTML = tickets.map((ticket) => {
        const active = ticket.id === activeTicketId ? 'active' : '';
        const time = formatJstDateTime(ticket.last_message_at || ticket.updated_at || ticket.created_at, {
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit'
        });
        const owner = showOwner ? `<span class="chat-task-owner">${escapeHtml(ticket.user_name || ticket.user_email || '-')}</span>` : '';
        return `
            <button type="button" class="chat-task-item ${active}" data-ticket-id="${escapeHtml(ticket.id)}" title="${escapeHtml(ticket.subject)}">
                <span class="chat-task-state status-${statusClass(ticket.status)}"></span>
                <span class="chat-task-title">${escapeHtml(truncateText(ticket.subject, 34))}</span>
                <span class="chat-task-time">${escapeHtml(time)}</span>
                ${owner}
            </button>
        `;
    }).join('');

    barEl.querySelectorAll('[data-ticket-id]').forEach((btn) => {
        btn.addEventListener('click', () => openTicket(btn.dataset.ticketId));
    });
}

function renderTickets() {
    renderTaskbar();

    const listEl = document.getElementById('chat-ticket-list');
    const countEl = document.getElementById('chat-ticket-count');
    if (!listEl) return;

    if (countEl) countEl.textContent = `${tickets.length}件`;

    if (!tickets.length) {
        listEl.innerHTML = '<div class="text-muted text-center" style="padding: 2rem;">問い合わせはまだありません</div>';
        return;
    }

    const showOwner = currentUser?.is_admin && getCurrentScope() === 'all';
    listEl.innerHTML = tickets.map((ticket) => {
        const active = ticket.id === activeTicketId ? 'active' : '';
        return `
            <button type="button" class="support-ticket-item ${active}" data-ticket-id="${escapeHtml(ticket.id)}">
                <div class="support-ticket-item-head">
                    <div class="support-ticket-item-title">${escapeHtml(ticket.subject)}</div>
                    <span class="support-status-badge status-${statusClass(ticket.status)}">${escapeHtml(formatStatus(ticket.status))}</span>
                </div>
                <div class="support-ticket-item-meta">
                    <span class="support-category-badge ${escapeHtml(ticket.category)}">${escapeHtml(formatCategory(ticket.category))}</span>
                    <small class="text-muted">${formatJstDateTime(ticket.last_message_at || ticket.updated_at || ticket.created_at)}</small>
                </div>
                ${showOwner ? `<div class="text-muted" style="font-size:0.78rem;">${escapeHtml(ticket.user_name || ticket.user_email || '-')}</div>` : ''}
                <div class="support-ticket-item-preview">${escapeHtml(truncateText(ticket.last_message || ''))}</div>
            </button>
        `;
    }).join('');

    listEl.querySelectorAll('[data-ticket-id]').forEach((btn) => {
        btn.addEventListener('click', () => openTicket(btn.dataset.ticketId));
    });
}

function resetThread() {
    activeTicketDetail = null;
    const titleEl = document.getElementById('chat-thread-title');
    const metaEl = document.getElementById('chat-thread-meta');
    const statusEl = document.getElementById('chat-thread-status');
    const messagesEl = document.getElementById('chat-thread-messages');
    const replyEl = document.getElementById('chat-reply-input');
    const sendBtn = document.getElementById('chat-send-reply-btn');
    const toggleBtn = document.getElementById('chat-toggle-status-btn');

    if (titleEl) titleEl.innerHTML = '<i class="fas fa-comments"></i> 問い合わせ詳細';
    if (metaEl) metaEl.textContent = '右のサイドバーから問い合わせを選択してください';
    if (statusEl) {
        statusEl.className = 'support-status-badge status-open';
        statusEl.textContent = '未選択';
    }
    if (messagesEl) {
        messagesEl.innerHTML = '<div class="text-muted text-center" style="padding: 2rem;">右のサイドバーから問い合わせを選択してください</div>';
    }
    if (replyEl) {
        replyEl.value = '';
        replyEl.disabled = true;
    }
    if (sendBtn) sendBtn.disabled = true;
    if (toggleBtn) {
        toggleBtn.disabled = true;
        toggleBtn.innerHTML = '<i class="fas fa-check"></i> 完了にする';
    }
}

function renderMessages(messages) {
    if (!Array.isArray(messages) || messages.length === 0) {
        return '<div class="text-muted text-center" style="padding: 2rem;">メッセージはまだありません</div>';
    }
    return messages.map((msg) => {
        const role = msg.sender_role === 'admin' ? 'admin' : 'user';
        const senderLabel = role === 'admin'
            ? (msg.user_id === currentUser.id ? '運営 (あなた)' : '運営')
            : (currentUser?.is_admin ? (msg.user_name || 'ユーザー') : 'あなた');
        return `
            <div class="support-message ${role}">
                <div class="support-message-head">
                    <span>${escapeHtml(senderLabel)}</span>
                    <span>${formatJstDateTime(msg.created_at)}</span>
                </div>
                <div class="support-message-body">${escapeHtml(msg.message)}</div>
            </div>
        `;
    }).join('');
}

function getToggleStatus(ticket) {
    if (!ticket) return 'closed';
    return ticket.status === 'closed' ? 'waiting_admin' : 'closed';
}

function syncAdminStatusSelect() {
    const statusSelect = document.getElementById('chat-admin-status');
    if (!statusSelect || !activeTicketDetail) return;
    statusSelect.value = activeTicketDetail.status;
}

async function openTicket(ticketId, rerenderList = true) {
    if (!ticketId) return;
    activeTicketId = ticketId;
    if (rerenderList) renderTickets();

    const data = await api(`/api/support/tickets/${ticketId}/messages`);
    activeTicketDetail = data.ticket;

    tickets = tickets.map((item) => (item.id === data.ticket.id ? { ...item, ...data.ticket } : item));

    const titleEl = document.getElementById('chat-thread-title');
    const metaEl = document.getElementById('chat-thread-meta');
    const statusEl = document.getElementById('chat-thread-status');
    const messagesEl = document.getElementById('chat-thread-messages');
    const replyEl = document.getElementById('chat-reply-input');
    const sendBtn = document.getElementById('chat-send-reply-btn');
    const toggleBtn = document.getElementById('chat-toggle-status-btn');

    if (titleEl) {
        titleEl.innerHTML = `<i class="fas fa-comments"></i> ${escapeHtml(data.ticket.subject)}`;
    }
    if (metaEl) {
        const ownerText = currentUser?.is_admin ? ` / ${data.ticket.user_name || data.ticket.user_email || data.ticket.user_id}` : '';
        metaEl.textContent = `${formatCategory(data.ticket.category)} / ${formatJstDateTime(data.ticket.created_at)}${ownerText}`;
    }
    if (statusEl) {
        statusEl.className = `support-status-badge status-${statusClass(data.ticket.status)}`;
        statusEl.textContent = formatStatus(data.ticket.status);
    }
    if (messagesEl) {
        messagesEl.innerHTML = renderMessages(data.messages);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    const isClosed = data.ticket.status === 'closed';
    if (replyEl) {
        replyEl.disabled = isClosed;
        replyEl.placeholder = isClosed ? '完了済みです。必要なら再オープンしてください。' : '返信を入力...';
    }
    if (sendBtn) sendBtn.disabled = isClosed;
    if (toggleBtn) {
        toggleBtn.disabled = false;
        toggleBtn.innerHTML = isClosed
            ? '<i class="fas fa-rotate-left"></i> 再オープン'
            : '<i class="fas fa-check"></i> 完了にする';
    }
    syncAdminStatusSelect();
    renderTickets();
}

async function loadTickets(keepSelection = true) {
    const taskbarEl = document.getElementById('chat-taskbar');
    if (taskbarEl) {
        taskbarEl.innerHTML = '<div class="text-muted text-center" style="padding: 0.8rem;">読み込み中...</div>';
    }

    const listEl = document.getElementById('chat-ticket-list');
    if (listEl) {
        listEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    }

    const scope = getCurrentScope();
    const endpoint = scope === 'all' ? '/api/support/tickets?scope=all' : '/api/support/tickets';
    tickets = await api(endpoint);
    renderTickets();

    if (!tickets.length) {
        activeTicketId = null;
        resetThread();
        return;
    }

    let nextId = activeTicketId;
    if (!keepSelection || !tickets.some((ticket) => ticket.id === nextId)) {
        nextId = requestedTicketId && tickets.some((ticket) => ticket.id === requestedTicketId)
            ? requestedTicketId
            : tickets[0].id;
    }
    requestedTicketId = null;
    if (nextId) {
        await openTicket(nextId, false);
    }
}

async function createTicket(event) {
    event.preventDefault();
    const category = document.getElementById('chat-category')?.value || 'inquiry';
    const subject = document.getElementById('chat-subject')?.value.trim() || '';
    const message = document.getElementById('chat-message')?.value.trim() || '';
    const submitBtn = event.target.querySelector('button[type="submit"]');

    if (subject.length < 2 || message.length < 2) {
        showToast('件名と本文を入力してください', 'warning');
        return;
    }

    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 送信中...';
    }
    try {
        const result = await api('/api/support/tickets', {
            method: 'POST',
            body: JSON.stringify({ category, subject, message })
        });
        showToast(result.message || 'お問い合わせを送信しました', 'success');
        event.target.reset();
        await loadTickets(false);
        if (result.id) {
            await openTicket(result.id);
        }
    } catch (error) {
        showToast(error.message, 'error');
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.innerHTML = '<i class="fas fa-paper-plane"></i> 送信';
        }
    }
}

async function sendReply() {
    if (!activeTicketId) return;
    const inputEl = document.getElementById('chat-reply-input');
    const sendBtn = document.getElementById('chat-send-reply-btn');
    const message = inputEl?.value.trim() || '';
    if (!message) {
        showToast('返信内容を入力してください', 'warning');
        return;
    }

    if (sendBtn) {
        sendBtn.disabled = true;
        sendBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 送信中...';
    }
    try {
        await api(`/api/support/tickets/${activeTicketId}/replies`, {
            method: 'POST',
            body: JSON.stringify({ message })
        });
        if (inputEl) inputEl.value = '';
        showToast('返信を送信しました', 'success');
        await loadTickets(true);
    } catch (error) {
        showToast(error.message, 'error');
    } finally {
        if (sendBtn) {
            sendBtn.disabled = false;
            sendBtn.innerHTML = '<i class="fas fa-reply"></i> 返信を送信';
        }
    }
}

async function updateStatus(nextStatus) {
    if (!activeTicketId) return;
    await api(`/api/support/tickets/${activeTicketId}/status`, {
        method: 'PUT',
        body: JSON.stringify({ status: nextStatus })
    });
    await loadTickets(true);
}

async function toggleStatus() {
    if (!activeTicketDetail) return;
    const next = getToggleStatus(activeTicketDetail);
    try {
        await updateStatus(next);
        showToast(next === 'closed' ? '完了にしました' : '再オープンしました', 'success');
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function updateAdminStatus() {
    const selectEl = document.getElementById('chat-admin-status');
    const next = selectEl?.value;
    if (!next) return;
    try {
        await updateStatus(next);
        showToast('状態を更新しました', 'success');
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function loadCurrentUser() {
    currentUser = await api('/api/auth/me');

    const userEl = document.getElementById('support-chat-user');
    const subtitleEl = document.getElementById('support-chat-subtitle');
    const scopeEl = document.getElementById('chat-scope-select');

    if (userEl) {
        userEl.textContent = `${currentUser.name || 'ユーザー'} (${currentUser.email || '-'})`;
    }
    if (subtitleEl) {
        subtitleEl.textContent = currentUser.is_admin
            ? '管理者として全問い合わせの閲覧・返信ができます'
            : '問い合わせと返信をこの画面で完結できます';
    }
    if (scopeEl && currentUser.is_admin && requestedScope === 'all') {
        scopeEl.value = 'all';
    }
    updateScopeVisibility();
}

function bindEvents() {
    document.getElementById('chat-create-form')?.addEventListener('submit', createTicket);
    document.getElementById('chat-taskbar-refresh-btn')?.addEventListener('click', () => loadTickets(true));
    document.getElementById('chat-send-reply-btn')?.addEventListener('click', sendReply);
    document.getElementById('chat-toggle-status-btn')?.addEventListener('click', toggleStatus);
    document.getElementById('chat-admin-status-btn')?.addEventListener('click', updateAdminStatus);
    document.getElementById('chat-scope-select')?.addEventListener('change', () => loadTickets(false));
    document.getElementById('chat-reply-input')?.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            sendReply();
        }
    });
    document.getElementById('support-chat-logout-btn')?.addEventListener('click', () => {
        localStorage.removeItem('token');
        localStorage.removeItem('user');
        window.location.href = '/auth.html';
    });
}

async function initialize() {
    try {
        bindEvents();
        await loadCurrentUser();
        await loadTickets(false);

        setInterval(() => {
            if (document.visibilityState === 'visible') {
                loadTickets(true).catch((error) => console.error('Auto refresh failed:', error));
            }
        }, 30000);
    } catch (error) {
        console.error('Support chat init error:', error);
        showToast(error.message || '初期化に失敗しました', 'error');
        if (String(error.message || '').includes('認証')) {
            localStorage.removeItem('token');
            window.location.href = '/auth.html';
        }
    }
}

initialize();
