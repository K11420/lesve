// Dashboard JavaScript - Production Version v2.3 (Mobile Support)
const API_URL = window.location.origin;

// URLパラメータからトークンを取得（Discord認証後のリダイレクト）
const urlParams = new URLSearchParams(window.location.search);
const urlTokens = urlParams.getAll('token')
    .map((v) => String(v || '').trim())
    .filter(Boolean);
const urlToken = urlTokens.length ? urlTokens[urlTokens.length - 1] : '';
if (urlToken) {
    localStorage.setItem('token', urlToken);
    // ログイン時刻を保存（トークン更新用）
    localStorage.setItem('token_time', Date.now().toString());
    // URLからトークンパラメータを削除
    urlParams.delete('token');
    const nextQuery = urlParams.toString();
    const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ''}${window.location.hash || ''}`;
    window.history.replaceState({}, document.title, nextUrl);
    
    // リダイレクト先があれば遷移
    const rawRedirect = sessionStorage.getItem('auth_redirect') || localStorage.getItem('auth_redirect') || '';
    let savedRedirect = '';
    if (rawRedirect) {
        try {
            const parsed = new URL(rawRedirect, window.location.origin);
            if (parsed.origin === window.location.origin && parsed.pathname !== '/auth.html') {
                parsed.searchParams.delete('token');
                parsed.searchParams.delete('redirect');
                const search = parsed.searchParams.toString();
                savedRedirect = `${parsed.pathname}${search ? `?${search}` : ''}${parsed.hash || ''}`;
            }
        } catch (e) {}
    }
    sessionStorage.removeItem('auth_redirect');
    localStorage.removeItem('auth_redirect');
    if (savedRedirect) {
        window.location.href = savedRedirect;
    }
}

const token = localStorage.getItem('token');
let user = JSON.parse(localStorage.getItem('user') || '{}');
let containers = [];
let serverTypes = [];
let currentPath = '/app';
let selectedContainerId = null;
let ws = null;
let terminalWs = null;
let currentTerminalContainer = null;
let currentDetailServer = null;  // 現在表示中のサーバー詳細
let detailTerminalWs = null;     // 詳細ページ用ターミナルWebSocket
let detailTtydUrl = '';          // 公開URL（固定）
let supportTickets = [];
let adminSupportTickets = [];
let activeSupportTicketId = null;
let activeAdminSupportTicketId = null;
let complianceIncidents = [];
let complianceSummary = null;
let complianceStatusFilter = 'open';
const PLAN_ORDER = ['free', 'standard', 'pro'];
const PLAN_LABELS = { free: 'Free', standard: 'Standard', pro: 'Pro' };
const SUPPORT_CATEGORY_LABELS = {
    bug_report: 'バグ報告',
    question: '質問',
    inquiry: 'お問い合わせ'
};
const SUPPORT_STATUS_LABELS = {
    open: '受付中',
    waiting_admin: '運営対応待ち',
    waiting_user: 'ユーザー返信待ち',
    closed: '完了'
};
const COMPLIANCE_STATUS_LABELS = {
    open: '未対応',
    investigating: '調査中',
    resolved: '解決済み',
    ignored: '誤検知/除外'
};
const COMPLIANCE_SEVERITY_LABELS = {
    low: '低',
    medium: '中',
    high: '高',
    critical: '重大'
};
const DASHBOARD_ROUTE_BY_PAGE = {
    overview: '/dashboard',
    servers: '/servers',
    console: '/console',
    files: '/files',
    env: '/env',
    backups: '/backups',
    patches: '/patches',
    settings: '/settings',
    admin: '/admin/dashboard'
};
const DASHBOARD_PAGE_BY_ROUTE = new Map(
    Object.entries(DASHBOARD_ROUTE_BY_PAGE).map(([page, route]) => [route, page])
);
DASHBOARD_PAGE_BY_ROUTE.set('/dashboard.html', 'overview');
const DISPLAY_LOCALE = 'ja-JP';
const DISPLAY_TIME_ZONE = 'Asia/Tokyo';

function normalizePathname(pathname) {
    const raw = String(pathname || '/').split('?')[0].split('#')[0] || '/';
    if (raw !== '/' && raw.endsWith('/')) {
        return raw.slice(0, -1);
    }
    return raw;
}

function safeDecodeURIComponent(value) {
    try {
        return decodeURIComponent(String(value || ''));
    } catch (e) {
        return String(value || '');
    }
}

function getRouteStateFromPath(pathname = window.location.pathname) {
    const normalized = normalizePathname(pathname);
    if (normalized.startsWith('/server/')) {
        const serverId = safeDecodeURIComponent(normalized.slice('/server/'.length)).trim();
        return {
            page: serverId ? 'server-detail' : 'servers',
            serverId: serverId || null
        };
    }

    const mappedPage = DASHBOARD_PAGE_BY_ROUTE.get(normalized);
    return {
        page: mappedPage || 'overview',
        serverId: null
    };
}

function buildPathForPage(pageName, serverId = null) {
    if (pageName === 'server-detail') {
        if (!serverId) return DASHBOARD_ROUTE_BY_PAGE.servers;
        return `/server/${encodeURIComponent(serverId)}`;
    }
    return DASHBOARD_ROUTE_BY_PAGE[pageName] || DASHBOARD_ROUTE_BY_PAGE.overview;
}

let pendingRouteState = getRouteStateFromPath();

function parseDisplayDate(value) {
    if (value === null || value === undefined || value === '') return null;
    const date = value instanceof Date ? value : new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
}

function formatJstDateTime(value, options = {}, fallback = '-') {
    const date = parseDisplayDate(value);
    if (!date) return fallback;
    return date.toLocaleString(DISPLAY_LOCALE, { timeZone: DISPLAY_TIME_ZONE, ...options });
}

function formatJstDate(value, options = {}, fallback = '-') {
    const date = parseDisplayDate(value);
    if (!date) return fallback;
    return date.toLocaleDateString(DISPLAY_LOCALE, { timeZone: DISPLAY_TIME_ZONE, ...options });
}

function formatJstTime(value = new Date(), options = {}, fallback = '-') {
    const date = parseDisplayDate(value);
    if (!date) return fallback;
    return date.toLocaleTimeString(DISPLAY_LOCALE, { timeZone: DISPLAY_TIME_ZONE, ...options });
}

// 認証チェック
if (!token) {
    const redirectParams = new URLSearchParams(window.location.search);
    redirectParams.delete('token');
    redirectParams.delete('redirect');
    const redirectQuery = redirectParams.toString();
    const redirectTarget = `${window.location.pathname}${redirectQuery ? `?${redirectQuery}` : ''}${window.location.hash || ''}`;
    sessionStorage.setItem('auth_redirect', redirectTarget);
    localStorage.setItem('auth_redirect', redirectTarget);
    window.location.replace(`/auth.html?redirect=${encodeURIComponent(redirectTarget)}`);
}

// ============ Mobile Sidebar Toggle ============
function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    sidebar.classList.toggle('open');
    overlay.classList.toggle('active');
    document.body.style.overflow = sidebar.classList.contains('open') ? 'hidden' : '';
}

function closeSidebar() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    sidebar.classList.remove('open');
    overlay.classList.remove('active');
    document.body.style.overflow = '';
}

function setActiveSidebarPage(pageName) {
    const sidebarPage = pageName === 'server-detail' ? 'servers' : pageName;
    document.querySelectorAll('.sidebar-nav a').forEach((a) => a.classList.remove('active'));
    document.querySelector(`.sidebar-nav a[data-page="${sidebarPage}"]`)?.classList.add('active');
}

// ページ遷移時にサイドバーを閉じる
function showPage(pageName, options = {}) {
    const {
        syncUrl = true,
        replaceHistory = false,
        serverId = null
    } = options;

    closeSidebar();
    if (pageName === 'support') {
        window.location.href = user?.is_admin ? '/chat/admin' : '/chat/user';
        return;
    }

    setActiveSidebarPage(pageName);

    // 全ページを非表示にして、対象ページを表示
    document.querySelectorAll('.page').forEach((p) => p.classList.remove('active'));
    const targetPage = document.getElementById(`page-${pageName}`);
    if (targetPage) {
        targetPage.classList.add('active');
    }

    if (syncUrl) {
        const detailServerId = serverId || (pageName === 'server-detail' ? currentDetailServer?.id : null);
        const nextPath = buildPathForPage(pageName, detailServerId);
        if (normalizePathname(window.location.pathname) !== normalizePathname(nextPath)) {
            const method = replaceHistory ? 'replaceState' : 'pushState';
            window.history[method]({}, document.title, nextPath);
        }
    }

    // 設定ページならSSH鍵とセキュリティ情報を読み込む
    if (pageName === 'settings' && typeof loadSshKeys === 'function') {
        loadSshKeys();
        if (typeof loadSecurityStatus === 'function') loadSecurityStatus();
    }
    if (pageName === 'patches') {
        loadPatchNotes();
    }
}

function applyRouteState(routeState, options = {}) {
    const route = routeState || { page: 'overview', serverId: null };
    const { syncUrl = false, replaceHistory = true, showMissingToast = true } = options;

    if (route.page === 'server-detail') {
        if (!route.serverId) {
            showPage('servers', { syncUrl, replaceHistory });
            return;
        }
        if (!containers.length) {
            pendingRouteState = route;
            return;
        }
        const target = containers.find((c) => c.id === route.serverId);
        if (!target) {
            showPage('servers', { syncUrl, replaceHistory });
            if (showMissingToast) {
                showToast('指定されたサーバーが見つかりません', 'warning');
            }
            return;
        }
        openServerDetail(route.serverId, { syncUrl, replaceHistory });
        return;
    }

    if (route.page === 'admin' && !user?.is_admin) {
        showPage('overview', { syncUrl, replaceHistory });
        return;
    }

    showPage(route.page, { syncUrl, replaceHistory });
    if (route.page === 'admin' && user?.is_admin) {
        loadAdminContent('users');
    }
}

function applyPendingRouteState(options = {}) {
    if (!pendingRouteState) return;
    const route = pendingRouteState;
    pendingRouteState = null;
    applyRouteState(route, options);
}

window.addEventListener('popstate', () => {
    pendingRouteState = getRouteStateFromPath();
    applyPendingRouteState({ syncUrl: false, replaceHistory: true, showMissingToast: false });
});

// ============ API Helper ============
async function api(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_URL}${endpoint}`, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`,
                ...options.headers
            }
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.error || 'APIエラーが発生しました');
        }
        
        return data;
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

// ============ Toast Notifications ============
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    
    const icons = {
        success: 'fa-check-circle',
        error: 'fa-exclamation-circle',
        warning: 'fa-exclamation-triangle',
        info: 'fa-info-circle'
    };
    
    toast.innerHTML = `
        <i class="fas ${icons[type]}"></i>
        <span>${message}</span>
    `;
    
    container.appendChild(toast);
    
    setTimeout(() => toast.classList.add('show'), 10);
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// エイリアス
const toast = showToast;

// ============ Clipboard ============
function copyToClipboard(text, element) {
    // navigator.clipboard はHTTPS環境でのみ動作するため、フォールバックを用意
    if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(() => {
            showCopySuccess(element);
        }).catch(() => {
            fallbackCopyToClipboard(text, element);
        });
    } else {
        fallbackCopyToClipboard(text, element);
    }
}

function fallbackCopyToClipboard(text, element) {
    // テキストエリアを使ったフォールバック
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        const successful = document.execCommand('copy');
        if (successful) {
            showCopySuccess(element);
        } else {
            showToast('コピーに失敗しました', 'error');
        }
    } catch (err) {
        showToast('コピーに失敗しました', 'error');
    }
    
    document.body.removeChild(textArea);
}

function showCopySuccess(element) {
    showToast('コピーしました', 'success');
    if (element) {
        element.classList.add('copied');
        setTimeout(() => element.classList.remove('copied'), 1000);
    }
}

function copySSHCommand(command, element) {
    copyToClipboard(command, element);
}

function copyPasswordFromData(element) {
    const password = element.dataset.password;
    if (password) {
        copyToClipboard(password, element);
    } else {
        showToast('パスワードが設定されていません', 'warning');
    }
}

// ============ User Info ============
async function loadUserInfo() {
    try {
        user = await api('/api/auth/me');
        localStorage.setItem('user', JSON.stringify(user));
        displayUserInfo();
    } catch (error) {
        console.error('Failed to load user info:', error);
        localStorage.removeItem('token');
        localStorage.removeItem('user');
        window.location.href = '/auth.html';
    }
}

function displayUserInfo() {
    document.getElementById('user-name').textContent = user.name || 'ユーザー';
    document.getElementById('user-email').textContent = user.email || '';
    
    // Discordアバター表示
    const avatarEl = document.getElementById('user-avatar');
    const mobileAvatarEl = document.getElementById('mobile-user-avatar');
    const avatarContent = user.avatar 
        ? `<img src="${user.avatar}" alt="avatar">`
        : (user.name || 'U').charAt(0).toUpperCase();
    
    if (user.avatar) {
        avatarEl.innerHTML = `<img src="${user.avatar}" alt="avatar" style="width:100%;height:100%;border-radius:50%;object-fit:cover;">`;
        if (mobileAvatarEl) mobileAvatarEl.innerHTML = `<img src="${user.avatar}" alt="avatar">`;
    } else {
        avatarEl.textContent = (user.name || 'U').charAt(0).toUpperCase();
        if (mobileAvatarEl) mobileAvatarEl.textContent = (user.name || 'U').charAt(0).toUpperCase();
    }
    
    const planBadge = document.getElementById('user-plan-badge');
    planBadge.textContent = (user.plan || 'free').toUpperCase();
    planBadge.className = `user-plan plan-${user.plan}`;
    
    // 管理者メニュー表示
    if (user.is_admin) {
        const adminLink = document.getElementById('admin-link');
        if (adminLink) adminLink.style.display = 'flex';
    }
    
    // 設定ページ
    if (document.getElementById('settings-name')) {
        document.getElementById('settings-name').value = user.name || '';
        document.getElementById('settings-email').value = user.email || '';
        document.getElementById('settings-created').value = formatJstDate(user.created_at, {}, '');
    }
    
    // ユーザー制限表示
    const limitInfo = document.getElementById('user-limits');
    if (limitInfo) {
        limitInfo.innerHTML = `
            <div>サーバー上限: ${user.max_containers === -1 ? '無制限' : user.max_containers + '台'}</div>
            <div>CPU: ${user.max_cpu} vCPU / メモリ: ${user.max_memory}MB / ストレージ: ${user.max_storage}MB</div>
        `;
    }

    updateCurrentPlanCard();
    updateCreateServerSpec();
    updateUpgradeButtonState();
}

function updateCurrentPlanCard() {
    const planInfo = document.getElementById('current-plan-info');
    if (!planInfo) return;
    const planName = PLAN_LABELS[user.plan] || PLAN_LABELS.free;

    planInfo.innerHTML = `
        <div class="plan-name">${planName}</div>
        <div class="plan-details">
            <div>CPU: ${user.max_cpu ?? 0.5} vCPU</div>
            <div>メモリ: ${formatResourceMB(user.max_memory ?? 512)}</div>
            <div>ストレージ: ${formatResourceMB(user.max_storage ?? 1024)}</div>
            <div>コンテナ: ${user.max_containers === -1 ? '無制限' : `${user.max_containers}個`}</div>
        </div>
    `;
}

function updateCreateServerSpec() {
    const specAlert = document.getElementById('server-spec-alert');
    const serverPlanEl = document.getElementById('server-plan');
    if (specAlert) {
        specAlert.innerHTML = `<i class="fas fa-microchip"></i> ${user.max_cpu ?? 0.5} vCPU / ${formatResourceMB(user.max_memory ?? 512)} メモリ / ${formatResourceMB(user.max_storage ?? 1024)} ストレージ`;
    }
    if (serverPlanEl) {
        serverPlanEl.value = user.plan || 'free';
    }
}

function getNextPlan(currentPlan) {
    const currentIndex = PLAN_ORDER.indexOf(currentPlan || 'free');
    if (currentIndex < 0 || currentIndex >= PLAN_ORDER.length - 1) return null;
    return PLAN_ORDER[currentIndex + 1];
}

function updateUpgradeButtonState() {
    const btn = document.getElementById('upgrade-plan-btn');
    if (!btn) return;
    const nextPlan = getNextPlan(user.plan);
    const portalBtn = document.getElementById('manage-subscription-btn');

    if (!nextPlan) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-check"></i> 最高プラン利用中';
    } else {
        btn.disabled = false;
        btn.innerHTML = `<i class="fas fa-arrow-up"></i> ${PLAN_LABELS[nextPlan]}へアップグレード`;
    }

    if (portalBtn) {
        const paidPlan = (user.plan || 'free') !== 'free';
        portalBtn.disabled = !paidPlan;
        portalBtn.innerHTML = paidPlan
            ? '<i class="fas fa-credit-card"></i> サブスク管理（解約/支払い）'
            : '<i class="fas fa-credit-card"></i> Freeプラン（管理対象なし）';
    }
}

async function startUpgradeCheckout() {
    const nextPlan = getNextPlan(user.plan);
    if (!nextPlan) {
        showToast('すでに最高プランです', 'info');
        return;
    }

    const btn = document.getElementById('upgrade-plan-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 決済画面へ移動中...';
    }

    try {
        const result = await api('/api/billing/checkout-session', {
            method: 'POST',
            body: JSON.stringify({ plan: nextPlan })
        });
        if (!result.url) {
            throw new Error('チェックアウトURLが取得できませんでした');
        }
        window.location.href = result.url;
    } catch (error) {
        showToast(`アップグレード開始に失敗しました: ${error.message}`, 'error');
        updateUpgradeButtonState();
    }
}

async function openBillingPortal() {
    const btn = document.getElementById('manage-subscription-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 管理ページ準備中...';
    }

    try {
        const result = await api('/api/billing/portal-session', {
            method: 'POST'
        });
        if (!result.url) {
            throw new Error('管理ページURLが取得できませんでした');
        }
        window.location.href = result.url;
    } catch (error) {
        showToast(`サブスク管理ページを開けませんでした: ${error.message}`, 'error');
        updateUpgradeButtonState();
    }
}

async function reapplyPlanLimits() {
    const btn = document.getElementById('reapply-plan-limits-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 再適用中...';
    }

    try {
        const result = await api('/api/billing/reapply-limits', {
            method: 'POST'
        });
        showToast(result.message || 'プラン値を再適用しました', 'success');
        await loadContainers();
        await loadStats();
    } catch (error) {
        showToast(`プラン再適用に失敗しました: ${error.message}`, 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-sync-alt"></i> 旧プラン値を再適用';
        }
    }
}

async function handleBillingRedirect() {
    const params = new URLSearchParams(window.location.search);
    const status = params.get('billing');
    const sessionId = params.get('session_id');
    if (!status) return;

    if (status === 'success' && sessionId) {
        try {
            const result = await api('/api/billing/confirm', {
                method: 'POST',
                body: JSON.stringify({ session_id: sessionId })
            });
            showToast(result.message || 'アップグレードが完了しました', 'success');
            await loadUserInfo();
            await loadContainers();
            await loadStats();
        } catch (error) {
            showToast(`決済確認に失敗しました: ${error.message}`, 'error');
        }
    } else if (status === 'cancel') {
        showToast('アップグレードをキャンセルしました', 'info');
    }

    params.delete('billing');
    params.delete('session_id');
    const nextQuery = params.toString();
    const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ''}`;
    window.history.replaceState({}, document.title, nextUrl);
}

// ============ Stats ============
async function loadStats() {
    try {
        const stats = await api('/api/stats');
        
        const statTotal = document.getElementById('stat-total');
        const statMax = document.getElementById('stat-max');
        const statRunning = document.getElementById('stat-running');
        const statStopped = document.getElementById('stat-stopped');
        const statCpu = document.getElementById('stat-cpu');
        const dockerStatus = document.getElementById('docker-status');
        
        if (statTotal) statTotal.textContent = stats.totalContainers || 0;
        if (statMax) statMax.textContent = stats.maxContainers === -1 ? '∞' : stats.maxContainers;
        if (statRunning) statRunning.textContent = stats.running || 0;
        if (statStopped) statStopped.textContent = stats.stopped || 0;
        if (statCpu) statCpu.textContent = `${stats.totalCpu || 0} vCPU`;
        
        // Docker状態
        if (dockerStatus) {
            if (stats.dockerAvailable) {
                dockerStatus.innerHTML = '<span class="status-indicator online"></span><span>Docker: 接続済み</span>';
            } else {
                dockerStatus.innerHTML = '<span class="status-indicator offline"></span><span>Docker: シミュレーション</span>';
            }
        }
    } catch (error) {
        console.error('Failed to load stats:', error);
        // エラー時もDocker状態を更新
        const dockerStatus = document.getElementById('docker-status');
        if (dockerStatus) {
            dockerStatus.innerHTML = '<span class="status-indicator offline"></span><span>Docker: エラー</span>';
        }
    }
}

// ============ Containers ============
async function loadContainers() {
    try {
        containers = await api('/api/containers');
        renderContainers('all-servers-list');
        updateServerSelects();
        renderServersContext();
        applyPendingRouteState({ syncUrl: false, replaceHistory: true, showMissingToast: false });
    } catch (error) {
        showToast('サーバー一覧の取得に失敗しました', 'error');
        const summaryEl = document.getElementById('context-backend-summary');
        if (summaryEl) summaryEl.textContent = '取得失敗';
        document.getElementById('all-servers-list').innerHTML = `
            <div class="empty-state" style="grid-column: 1 / -1;">
                <i class="fas fa-exclamation-triangle"></i>
                <h3>読み込みエラー</h3>
                <p>${error.message}</p>
            </div>
        `;
    }
}

function getBackendType(server) {
    if (server.backend_type) return server.backend_type;
    return String(server.docker_id || '').startsWith('vm:') ? 'vm' : 'docker';
}

function formatResourceMB(megabytes) {
    const mb = Number(megabytes) || 0;
    if (mb >= 1024) {
        const gb = mb / 1024;
        return `${Number.isInteger(gb) ? gb : gb.toFixed(1)} GB`;
    }
    return `${mb} MB`;
}

function formatCpuLimit(cpuLimit) {
    const cpu = Number(cpuLimit) || 0;
    return Number.isInteger(cpu) ? `${cpu} vCPU` : `${cpu.toFixed(1)} vCPU`;
}

function clampPercent(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return 0;
    return Math.min(n, 100);
}

function formatPercent(value, digits = 1) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '-';
    const clamped = clampPercent(n);
    return clamped.toFixed(digits);
}

function hasVmSpecDiff(server) {
    const configuredCpuLimit = Number(server.configured_cpu_limit ?? server.cpu_limit);
    const configuredMemoryLimit = Number(server.configured_memory_limit ?? server.memory_limit);
    const configuredStorageLimit = Number(server.configured_storage_limit ?? server.storage_limit);
    const actualCpuLimit = Number(server.cpu_limit);
    const actualMemoryLimit = Number(server.memory_limit);
    const actualStorageLimit = Number(server.storage_limit);

    const cpuDiff = configuredCpuLimit !== actualCpuLimit;
    const memoryDiff = configuredMemoryLimit !== actualMemoryLimit;
    // VMディスクは縮小不可のため、実機が大きい差分は警告しない
    const storageShortage = Number.isFinite(actualStorageLimit) && Number.isFinite(configuredStorageLimit)
        ? actualStorageLimit < configuredStorageLimit
        : configuredStorageLimit !== actualStorageLimit;

    return cpuDiff || memoryDiff || storageShortage;
}

function escapeJsSingleQuoted(text) {
    return String(text ?? '')
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'")
        .replace(/\r/g, '')
        .replace(/\n/g, '\\n');
}

function normalizeAppLikePath(pathValue) {
    const value = String(pathValue || '').replace(/\\/g, '/').replace(/^\/+/, '');
    return value ? `/app/${value}` : '/app';
}

function renderServersContext() {
    const summaryEl = document.getElementById('context-backend-summary');
    if (!summaryEl) return;

    if (containers.length === 0) {
        summaryEl.textContent = '未作成';
        return;
    }

    const vmCount = containers.filter((c) => getBackendType(c) === 'vm').length;
    const dockerCount = containers.length - vmCount;
    summaryEl.textContent = `VPSモード ${vmCount}台 / コンテナ ${dockerCount}台`;
}

function renderContainers(elementId) {
    const container = document.getElementById(elementId);
    if (!container) return;
    
    if (containers.length === 0) {
        container.innerHTML = `
            <div class="empty-state" style="grid-column: 1 / -1;">
                <i class="fas fa-server"></i>
                <h3>サーバーがありません</h3>
                <p>「新規作成」ボタンからサーバーを作成してください</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = containers.map((server) => {
        const backendType = getBackendType(server);
        const isVmBackend = backendType === 'vm';
        const resourceSourceLabel = server.resource_source === 'vm' ? '実機スペック（適用中）' : '設定スペック';
        const cpuUsage = server.stats?.cpuUsage == null ? null : clampPercent(server.stats.cpuUsage);
        const memoryUsage = server.stats?.memoryUsage == null ? null : clampPercent(server.stats.memoryUsage);
        const diskPercent = server.stats?.diskUsagePercent == null ? null : clampPercent(server.stats.diskUsagePercent);
        const cpuUsageText = cpuUsage === null ? '-' : `${formatPercent(cpuUsage)}%`;
        const memoryUsageText = memoryUsage === null ? '-' : `${formatPercent(memoryUsage)}%`;
        const diskPercentClass = diskPercent === null ? 'idle' : (diskPercent > 80 ? 'danger' : diskPercent > 60 ? 'warning' : '');
        const diskUsageText = server.stats?.diskUsage || '-';
        const networkInText = server.stats?.networkIn || '-';
        const networkOutText = server.stats?.networkOut || '-';
        const configuredCpuLimit = server.configured_cpu_limit ?? server.cpu_limit;
        const configuredMemoryLimit = server.configured_memory_limit ?? server.memory_limit;
        const configuredStorageLimit = server.configured_storage_limit ?? server.storage_limit;

        const hasSpecDiff = isVmBackend && hasVmSpecDiff(server);

        const configuredSpecText = `${formatCpuLimit(configuredCpuLimit)} / ${formatResourceMB(configuredMemoryLimit)} / ${formatResourceMB(configuredStorageLimit)}`;
        const escapedServerName = escapeHtml(server.name);
        const escapedServerNameJs = escapeJsSingleQuoted(server.name);
        const sshCommand = server.ssh_command || '';
        const sshHost = server.ssh_host || '';
        const sshUser = server.ssh_user || '';

        return `
            <div class="server-card" data-id="${server.id}" onclick="openServerDetail('${server.id}')" style="cursor: pointer;">
                <div class="server-header">
                    <div class="server-info">
                        <h3>${escapedServerName}</h3>
                        <div class="server-meta">
                            <span class="server-type">
                                <i class="${getTypeIcon(server.type)}"></i> ${server.type}
                            </span>
                            <span class="server-plan-badge plan-${server.plan}">${server.plan}</span>
                        </div>
                    </div>
                    <span class="server-status ${server.status}">
                        <span class="status-dot"></span>
                        ${server.status === 'running' ? '稼働中' : '停止中'}
                    </span>
                </div>
                <div class="server-capacity-grid">
                    <div class="capacity-item">
                        <span class="capacity-label">CPU</span>
                        <span class="capacity-value">${formatCpuLimit(server.cpu_limit)}</span>
                    </div>
                    <div class="capacity-item">
                        <span class="capacity-label">メモリ</span>
                        <span class="capacity-value">${formatResourceMB(server.memory_limit)}</span>
                    </div>
                    <div class="capacity-item">
                        <span class="capacity-label">ストレージ</span>
                        <span class="capacity-value">${formatResourceMB(server.storage_limit)}</span>
                    </div>
                    <div class="capacity-item">
                        <span class="capacity-label">モード</span>
                        <span class="capacity-value ${isVmBackend ? 'is-vm' : ''}">${isVmBackend ? 'VPSモード' : 'コンテナ'}</span>
                    </div>
                </div>
                <div class="server-mode-row">
                    <span class="backend-badge ${isVmBackend ? 'backend-vm' : 'backend-docker'}">
                        <i class="fas ${isVmBackend ? 'fa-cube' : 'fa-box'}"></i>
                        ${isVmBackend ? 'KVM / libvirt' : 'Docker'}
                    </span>
                    <span class="resource-source-badge">${resourceSourceLabel}</span>
                </div>
                ${hasSpecDiff ? `
                    <div class="server-spec-note">
                        <i class="fas fa-pen-ruler"></i> 旧プラン値（未適用）: ${configuredSpecText}
                    </div>
                ` : ''}
                <div class="server-stats">
                    <div class="server-stat">
                        <label>CPU使用率</label>
                        <div class="progress-bar">
                            <div class="progress ${cpuUsage === null ? 'idle' : ''}" style="width: ${cpuUsage ?? 0}%"></div>
                        </div>
                        <span>${cpuUsageText}</span>
                    </div>
                    <div class="server-stat">
                        <label>メモリ使用率</label>
                        <div class="progress-bar">
                            <div class="progress ${memoryUsage === null ? 'idle' : ''}" style="width: ${memoryUsage ?? 0}%"></div>
                        </div>
                        <span>${memoryUsageText}</span>
                    </div>
                    <div class="server-stat">
                        <label>ディスク使用量</label>
                        <div class="progress-bar">
                            <div class="progress ${diskPercentClass}" style="width: ${diskPercent ?? 0}%"></div>
                        </div>
                        <span>${diskUsageText} / ${formatResourceMB(server.storage_limit)}</span>
                    </div>
                    <div class="server-stat">
                        <label>ネットワーク</label>
                        <span class="network-info">↓${networkInText} ↑${networkOutText}</span>
                    </div>
                </div>
                ${server.ssh_user ? `
                <div class="server-ssh-info" onclick="event.stopPropagation()">
                    <div class="ssh-label"><i class="fas fa-terminal"></i> SSH接続 (ポート ${server.ssh_port || 4104})</div>
                    <div class="ssh-details">
                        <div class="ssh-row">
                            <span class="ssh-row-label">コマンド:</span>
                            <code class="ssh-value copyable" onclick="copyToClipboard('${escapeJsSingleQuoted(sshCommand)}', this)" title="クリックでコピー">
                                ${escapeHtml(sshCommand || '-')}
                            </code>
                        </div>
                        <div class="ssh-row">
                            <span class="ssh-row-label">ホスト:</span>
                            <code class="ssh-value copyable" onclick="copyToClipboard('${escapeJsSingleQuoted(sshHost)}', this)" title="クリックでコピー">
                                ${escapeHtml(sshHost || '-')}
                            </code>
                        </div>
                        <div class="ssh-row">
                            <span class="ssh-row-label">ユーザー:</span>
                            <code class="ssh-value copyable" onclick="copyToClipboard('${escapeJsSingleQuoted(sshUser)}', this)" title="クリックでコピー">
                                ${escapeHtml(sshUser || '-')}
                            </code>
                        </div>
                        <div class="ssh-row">
                            <span class="ssh-row-label">パスワード:</span>
                            <code class="ssh-value copyable password-field" data-password="${escapeHtml(server.ssh_password || '')}" onclick="copyPasswordFromData(this)" title="クリックでコピー">
                                ${server.ssh_password ? '●●●●●●●●' : '-'}
                            </code>
                        </div>
                    </div>
                </div>
                ` : ''}
                <div class="server-actions" onclick="event.stopPropagation()">
                    ${server.status === 'running' ? `
                        <button class="btn btn-warning btn-small" onclick="stopContainer('${server.id}')" title="停止">
                            <i class="fas fa-stop"></i>
                        </button>
                        <button class="btn btn-secondary btn-small" onclick="restartContainer('${server.id}')" title="再起動">
                            <i class="fas fa-redo"></i>
                        </button>
                    ` : `
                        <button class="btn btn-success btn-small" onclick="startContainer('${server.id}')" title="起動">
                            <i class="fas fa-play"></i>
                        </button>
                    `}
                    <button class="btn btn-primary btn-small" onclick="openServerDetail('${server.id}')" title="詳細">
                        <i class="fas fa-cog"></i>
                    </button>
                    <button class="btn btn-danger btn-small" onclick="confirmDelete('${server.id}', '${escapedServerNameJs}')" title="削除">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        `;
    }).join('');
}

function updateServerSelects() {
    const selects = ['console-server-select', 'file-server-select', 'env-server-select', 'backup-server-select'];
    
    selects.forEach(selectId => {
        const select = document.getElementById(selectId);
        if (!select) return;
        
        const currentValue = select.value;
        select.innerHTML = '<option value="">サーバーを選択...</option>' + 
            containers.map(c => `<option value="${c.id}">${escapeHtml(c.name)} (${c.status})</option>`).join('');
        
        if (currentValue && containers.find(c => c.id === currentValue)) {
            select.value = currentValue;
        }
    });
}

function getTypeIcon(type) {
    const icons = {
        'ubuntu': 'fab fa-ubuntu',
        'ubuntu-nodejs': 'fab fa-node-js',
        'ubuntu-python': 'fab fa-python',
        'ubuntu-java': 'fab fa-java',
        'ubuntu-go': 'fas fa-code',
        'ubuntu-full': 'fas fa-layer-group',
        'nodejs': 'fab fa-node-js',
        'python': 'fab fa-python',
        'java': 'fab fa-java',
        'go': 'fas fa-code'
    };
    return icons[type] || 'fab fa-ubuntu';
}

// ============ Server Types ============
async function loadServerTypes() {
    try {
        serverTypes = await api('/api/server-types');
        renderServerTypes();
    } catch (error) {
        console.error('Failed to load server types:', error);
    }
}

function renderServerTypes() {
    const grid = document.getElementById('server-type-grid');
    if (!grid) return;
    
    grid.innerHTML = serverTypes.map(type => `
        <div class="server-type-option ${type.id === 'ubuntu' ? 'selected' : ''}" data-type="${type.id}">
            <i class="${getTypeIcon(type.id)} type-icon"></i>
            <div class="type-name">${type.name}</div>
            <div class="type-desc">${type.description}</div>
        </div>
    `).join('');
    
    // クリックイベント
    grid.querySelectorAll('.server-type-option').forEach(option => {
        option.addEventListener('click', () => {
            grid.querySelectorAll('.server-type-option').forEach(o => o.classList.remove('selected'));
            option.classList.add('selected');
            document.getElementById('server-type').value = option.dataset.type;
        });
    });
}

// ============ Container Operations ============
async function startContainer(id) {
    try {
        await api(`/api/containers/${id}/start`, { method: 'POST' });
        showToast('サーバーを起動しました', 'success');
        await loadContainers();
        await loadStats();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function stopContainer(id) {
    try {
        await api(`/api/containers/${id}/stop`, { method: 'POST' });
        showToast('サーバーを停止しました', 'success');
        await loadContainers();
        await loadStats();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function restartContainer(id) {
    try {
        await api(`/api/containers/${id}/restart`, { method: 'POST' });
        showToast('サーバーを再起動しました', 'success');
        await loadContainers();
        await loadStats();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

function confirmDelete(id, name) {
    document.getElementById('confirm-title').textContent = 'サーバー削除';
    document.getElementById('confirm-message').textContent = `"${name}" を削除しますか？この操作は取り消せません。`;
    document.getElementById('confirm-btn').onclick = () => deleteContainer(id);
    document.getElementById('confirm-modal').classList.add('active');
}

async function deleteContainer(id) {
    closeConfirmModal();
    try {
        await api(`/api/containers/${id}`, { method: 'DELETE' });
        showToast('サーバーを削除しました', 'success');
        await loadContainers();
        await loadStats();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

// ============ Console ============
function viewLogs(id) {
    navigateTo('console');
    document.getElementById('console-server-select').value = id;
    loadLogs(id);
}

async function loadLogs(id) {
    if (!id) return;
    
    const output = document.getElementById('console-output');
    output.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    
    try {
        const data = await api(`/api/containers/${id}/logs`);
        output.innerHTML = formatLogs(data.logs);
        output.scrollTop = output.scrollHeight;
    } catch (error) {
        output.innerHTML = `<div class="error">${error.message}</div>`;
    }
}

function formatLogs(logs) {
    return logs.split('\n').map(line => {
        let className = '';
        if (line.match(/error|Error|ERROR|fail|Fail|FAIL/i)) className = 'error';
        else if (line.match(/warn|Warn|WARN/i)) className = 'warning';
        else if (line.match(/success|ready|started|listening|OK/i)) className = 'success';
        else if (line.match(/info|Info|INFO/i)) className = 'info';
        
        return `<div class="log-line ${className}">${escapeHtml(line)}</div>`;
    }).join('');
}

// WebSocket接続 (ログ用)
function connectWebSocket() {
    if (ws) ws.close();
    
    const wsUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws?token=${token}`;
    ws = new WebSocket(wsUrl);
    
    ws.onopen = () => {
        console.log('WebSocket connected');
    };
    
    ws.onclose = () => {
        setTimeout(connectWebSocket, 5000);
    };
    
    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'output') {
                const output = document.getElementById('console-output');
                if (output) {
                    output.innerHTML += formatLogs(data.data);
                    output.scrollTop = output.scrollHeight;
                }
            }
        } catch (e) {}
    };
}

// ターミナルWebSocket接続
function connectTerminal(containerId) {
    if (terminalWs) {
        terminalWs.close();
    }
    
    currentTerminalContainer = containerId;
    const wsUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws?token=${token}&container=${containerId}`;
    
    const terminalOutput = document.getElementById('terminal-output');
    const terminalInput = document.getElementById('terminal-input');
    const wsStatus = document.getElementById('ws-status');

    terminalOutput.innerHTML = '<div class="terminal-welcome">接続中...</div>';
    wsStatus.innerHTML = '<span class="status-dot connecting"></span> 接続中';
    
    terminalWs = new WebSocket(wsUrl);
    
    terminalWs.onopen = () => {
        wsStatus.innerHTML = '<span class="status-dot online"></span> 接続済み';
        terminalInput.disabled = false;
        terminalInput.focus();
        terminalOutput.innerHTML = '';
        appendTerminalLine('system', `接続しました - Ubuntu Terminal`);
        appendTerminalLine('system', 'コマンドを入力してEnterで実行');
        appendTerminalLine('system', '---');
    };
    
    terminalWs.onclose = () => {
        wsStatus.innerHTML = '<span class="status-dot offline"></span> 切断';
        terminalInput.disabled = true;
        appendTerminalLine('system', '接続が切断されました');
    };
    
    terminalWs.onerror = (error) => {
        appendTerminalLine('error', 'エラー: 接続に失敗しました');
    };
    
    terminalWs.onmessage = (event) => {
        try {
            // HTMLレスポンスチェック（エラー時）
            if (event.data.startsWith('<!DOCTYPE') || event.data.startsWith('<html')) {
                appendTerminalLine('error', 'サーバーエラー: 接続を再試行してください');
                return;
            }
            
            const data = JSON.parse(event.data);
            
            switch (data.type) {
                case 'connected':
                    appendTerminalLine('success', data.message);
                    break;
                case 'output':
                    appendTerminalOutput(data.data);
                    break;
                case 'error':
                    if (data.action === 'restart_required') {
                        // コンテナが存在しない場合、再起動ボタンを表示
                        appendTerminalLine('error', data.message);
                        appendRestartButton(currentTerminalContainer);
                    } else {
                        appendTerminalLine('error', data.message);
                    }
                    break;
                case 'info':
                    appendTerminalLine('info', data.message);
                    break;
                case 'disconnect':
                    appendTerminalLine('warning', data.message);
                    break;
            }
        } catch (e) {
            // 生のテキストとして処理（ターミナル出力）
            if (event.data && !event.data.includes('<!DOCTYPE')) {
                appendTerminalOutput(event.data);
            }
        }
    };
}

function appendTerminalLine(type, text) {
    const output = document.getElementById('terminal-output');
    const line = document.createElement('div');
    line.className = `terminal-line terminal-${type}`;
    line.textContent = text;
    output.appendChild(line);
    output.scrollTop = output.scrollHeight;
}

// ターミナルに再起動ボタンを表示
function appendRestartButton(containerId) {
    const output = document.getElementById('terminal-output');
    const btnContainer = document.createElement('div');
    btnContainer.className = 'terminal-action';
    btnContainer.style.cssText = 'margin: 15px 0; padding: 15px; background: rgba(255,165,0,0.1); border-radius: 8px; text-align: center;';
    
    const btn = document.createElement('button');
    btn.className = 'btn btn-warning';
    btn.innerHTML = '<i class="fas fa-play"></i> コンテナを再作成して起動';
    btn.style.cssText = 'padding: 10px 20px; font-size: 14px;';
    btn.onclick = async () => {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 起動中...';
        
        try {
            const result = await api(`/api/containers/${containerId}/start`, { method: 'POST' });
            if (result.success) {
                toast('サーバーを起動しました', 'success');
                btnContainer.innerHTML = '<span style="color: #4caf50;"><i class="fas fa-check"></i> 起動完了！再接続してください</span>';
                
                // 少し待ってから再接続を試みる
                setTimeout(() => {
                    connectTerminal(containerId);
                }, 2000);
                
                // コンテナ一覧を更新
                loadContainers();
            }
        } catch (error) {
            toast('起動に失敗しました: ' + error.message, 'error');
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-play"></i> コンテナを再作成して起動';
        }
    };
    
    btnContainer.appendChild(btn);
    output.appendChild(btnContainer);
    output.scrollTop = output.scrollHeight;
}

function appendTerminalOutput(text) {
    const output = document.getElementById('terminal-output');
    
    // ANSIエスケープシーケンスを全て除去
    let cleanText = text
        // 色・スタイル
        .replace(/\x1b\[[0-9;]*m/g, '')
        // カーソル移動
        .replace(/\x1b\[[0-9;]*[ABCDHJ]/g, '')
        // タイトル設定
        .replace(/\x1b\][0-9];[^\x07]*\x07/g, '')
        // その他の制御シーケンス
        .replace(/\x1b\[\?[0-9;]*[hl]/g, '')
        // 制御文字（SOH, STX等）
        .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '')
        // キャリッジリターン
        .replace(/\r/g, '');
    
    const lines = cleanText.split('\n');
    lines.forEach(line => {
        // プロンプトを検出して整形
        const trimmedLine = line.trim();
        if (trimmedLine) {
            const lineDiv = document.createElement('div');
            lineDiv.className = 'terminal-line';
            
            // root@hostname:~# 形式のプロンプトを検出
            if (trimmedLine.match(/^(root|user)@[^:]+:[^#$]*[#$]\s*$/)) {
                lineDiv.className += ' terminal-prompt';
            }
            
            lineDiv.textContent = trimmedLine;
            output.appendChild(lineDiv);
        }
    });
    output.scrollTop = output.scrollHeight;
}

function sendTerminalInput(input) {
    if (terminalWs && terminalWs.readyState === WebSocket.OPEN) {
        terminalWs.send(JSON.stringify({ type: 'input', data: input + '\n' }));
    }
}

function disconnectTerminal() {
    if (terminalWs) {
        terminalWs.close();
        terminalWs = null;
    }
    currentTerminalContainer = null;
    
    const terminalInput = document.getElementById('terminal-input');
    const wsStatus = document.getElementById('ws-status');
    
    if (terminalInput) terminalInput.disabled = true;
    if (wsStatus) wsStatus.innerHTML = '<span class="status-dot offline"></span> 未接続';
}

// ============ Files ============
function viewFiles(id) {
    navigateTo('files');
    document.getElementById('file-server-select').value = id;
    selectedContainerId = id;
    currentPath = '/app';
    loadFiles(id, '/app');
}

async function loadFiles(containerId, path) {
    if (!containerId) return;

    const normalizedPath = !path
        ? '/app'
        : (path.startsWith('/') ? path : normalizeAppLikePath(path));

    selectedContainerId = containerId;
    currentPath = normalizedPath;
    
    const fileList = document.getElementById('file-list');
    fileList.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    
    document.getElementById('current-path').textContent = normalizedPath;
    
    try {
        const data = await api(`/api/containers/${containerId}/files?path=${encodeURIComponent(normalizedPath)}`);
        
        let html = '';
        
        // 親フォルダへ
        if (normalizedPath !== '/app') {
            const parentPath = normalizedPath.substring(0, normalizedPath.lastIndexOf('/')) || '/app';
            html += `
                <div class="file-item" ondblclick="loadFiles('${containerId}', '${parentPath}')">
                    <i class="fas fa-folder file-icon folder"></i>
                    <span class="file-name">..</span>
                    <span class="file-size">-</span>
                    <span class="file-modified">-</span>
                    <div class="file-item-actions"></div>
                </div>
            `;
        }
        
        // ファイル・フォルダ一覧
        data.items.sort((a, b) => {
            if (a.type !== b.type) return a.type === 'directory' ? -1 : 1;
            return a.name.localeCompare(b.name);
        }).forEach(item => {
            const fullPath = normalizedPath === '/' ? `/${item.name}` : `${normalizedPath}/${item.name}`;
            const icon = item.type === 'directory' ? 'fa-folder folder' : getFileIcon(item.name);
            const size = item.type === 'directory' ? '-' : formatFileSize(item.size);
            const modified = formatJstDateTime(item.modified);
            
            html += `
                <div class="file-item" ondblclick="${item.type === 'directory' ? 
                    `loadFiles('${containerId}', '${fullPath}')` : 
                    `editFile('${fullPath}')`}">
                    <i class="fas ${icon} file-icon"></i>
                    <span class="file-name">${escapeHtml(item.name)}</span>
                    <span class="file-size">${size}</span>
                    <span class="file-modified">${modified}</span>
                    <div class="file-item-actions">
                        ${item.type === 'file' ? `
                            <button class="btn btn-icon btn-small" onclick="event.stopPropagation(); editFile('${fullPath}')" title="編集">
                                <i class="fas fa-edit"></i>
                            </button>
                        ` : ''}
                        <button class="btn btn-icon btn-small btn-danger" onclick="event.stopPropagation(); deleteFile('${fullPath}')" title="削除">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </div>
            `;
        });
        
        if (data.items.length === 0 && normalizedPath === '/app') {
            html = `
                <div class="empty-state">
                    <i class="fas fa-folder-open"></i>
                    <h3>ファイルがありません</h3>
                    <p>ファイルをアップロードするか、新規作成してください</p>
                </div>
            `;
        }
        
        fileList.innerHTML = html;
    } catch (error) {
        fileList.innerHTML = `<div class="error">${error.message}</div>`;
    }
}

function getFileIcon(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    const icons = {
        js: 'fa-js text-yellow',
        ts: 'fa-code text-blue',
        py: 'fa-python text-blue',
        java: 'fa-java text-orange',
        json: 'fa-code text-green',
        md: 'fa-markdown',
        html: 'fa-html5 text-orange',
        css: 'fa-css3 text-blue',
        txt: 'fa-file-alt',
        log: 'fa-file-alt',
        sh: 'fa-terminal',
        yml: 'fa-file-code',
        yaml: 'fa-file-code',
        env: 'fa-key'
    };
    return icons[ext] || 'fa-file';
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

async function editFile(filePath) {
    try {
        const data = await api(`/api/containers/${selectedContainerId}/files/content?path=${encodeURIComponent(filePath)}`);
        
        document.getElementById('editor-filename').textContent = filePath.split('/').pop();
        document.getElementById('file-editor').value = data.content;
        document.getElementById('file-editor').dataset.path = filePath;
        document.getElementById('editor-modal').classList.add('active');
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function saveFile() {
    const editor = document.getElementById('file-editor');
    const filePath = editor.dataset.path;
    const content = editor.value;
    
    try {
        await api(`/api/containers/${selectedContainerId}/files/content`, {
            method: 'POST',
            body: JSON.stringify({ path: filePath, content })
        });
        
        showToast('ファイルを保存しました', 'success');
        closeEditorModal();
        loadFiles(selectedContainerId, currentPath);
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function deleteFile(filePath) {
    if (!confirm(`"${filePath.split('/').pop()}" を削除しますか？`)) return;
    
    try {
        await api(`/api/containers/${selectedContainerId}/files`, {
            method: 'DELETE',
            body: JSON.stringify({ path: filePath })
        });
        
        showToast('削除しました', 'success');
        loadFiles(selectedContainerId, currentPath);
    } catch (error) {
        showToast(error.message, 'error');
    }
}

function createNewFile() {
    const filename = prompt('ファイル名を入力:');
    if (!filename) return;
    
    const filePath = currentPath ? `${currentPath}/${filename}` : filename;
    document.getElementById('editor-filename').textContent = filename;
    document.getElementById('file-editor').value = '';
    document.getElementById('file-editor').dataset.path = filePath;
    document.getElementById('editor-modal').classList.add('active');
}

function createNewFolder() {
    const foldername = prompt('フォルダ名を入力:');
    if (!foldername) return;
    
    const folderPath = currentPath ? `${currentPath}/${foldername}` : foldername;
    
    api(`/api/containers/${selectedContainerId}/files/content`, {
        method: 'POST',
        body: JSON.stringify({ path: `${folderPath}/.gitkeep`, content: '' })
    }).then(() => {
        showToast('フォルダを作成しました', 'success');
        loadFiles(selectedContainerId, currentPath);
    }).catch(error => {
        showToast(error.message, 'error');
    });
}

// ============ Environment Variables ============
async function loadEnvVars(containerId) {
    if (!containerId) return;
    
    const envContainer = document.getElementById('env-container');
    
    try {
        const container = await api(`/api/containers/${containerId}`);
        const envVars = container.env_vars || {};
        
        let html = `
            <div class="env-editor">
                <div class="env-list" id="env-list">
        `;
        
        Object.entries(envVars).forEach(([key, value], index) => {
            html += `
                <div class="env-row" data-index="${index}">
                    <input type="text" class="form-input env-key" value="${escapeHtml(key)}" placeholder="KEY">
                    <input type="text" class="form-input env-value" value="${escapeHtml(value)}" placeholder="value">
                    <button class="btn btn-icon btn-danger" onclick="removeEnvVar(this)">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
            `;
        });
        
        html += `
                </div>
                <button class="btn btn-secondary" onclick="addEnvVar()">
                    <i class="fas fa-plus"></i> 変数を追加
                </button>
                <button class="btn btn-primary" onclick="saveEnvVars('${containerId}')">
                    <i class="fas fa-save"></i> 保存
                </button>
            </div>
            <div class="alert alert-info mt-2">
                <i class="fas fa-info-circle"></i>
                環境変数の変更を反映するには、サーバーの再起動が必要です。
            </div>
        `;
        
        envContainer.innerHTML = html;
    } catch (error) {
        envContainer.innerHTML = `<div class="alert alert-error">${error.message}</div>`;
    }
}

function addEnvVar() {
    const envList = document.getElementById('env-list');
    const index = envList.children.length;
    
    const row = document.createElement('div');
    row.className = 'env-row';
    row.dataset.index = index;
    row.innerHTML = `
        <input type="text" class="form-input env-key" placeholder="KEY">
        <input type="text" class="form-input env-value" placeholder="value">
        <button class="btn btn-icon btn-danger" onclick="removeEnvVar(this)">
            <i class="fas fa-times"></i>
        </button>
    `;
    
    envList.appendChild(row);
}

function removeEnvVar(btn) {
    btn.parentElement.remove();
}

async function saveEnvVars(containerId) {
    const rows = document.querySelectorAll('#env-list .env-row');
    const envVars = {};
    
    rows.forEach(row => {
        const key = row.querySelector('.env-key').value.trim();
        const value = row.querySelector('.env-value').value;
        if (key) envVars[key] = value;
    });
    
    try {
        await api(`/api/containers/${containerId}/env`, {
            method: 'PUT',
            body: JSON.stringify({ env_vars: envVars })
        });
        
        showToast('環境変数を保存しました', 'success');
    } catch (error) {
        showToast(error.message, 'error');
    }
}

// ============ Backups ============
async function loadBackups(containerId) {
    if (!containerId) {
        document.getElementById('create-backup-btn').disabled = true;
        return;
    }
    
    document.getElementById('create-backup-btn').disabled = false;
    document.getElementById('create-backup-btn').onclick = () => createBackup(containerId);
    
    const backupList = document.getElementById('backup-list');
    backupList.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    
    try {
        const backups = await api(`/api/containers/${containerId}/backups`);
        
        if (backups.length === 0) {
            backupList.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-archive"></i>
                    <h3>バックアップがありません</h3>
                    <p>「バックアップ作成」ボタンでバックアップを作成できます</p>
                </div>
            `;
            return;
        }
        
        backupList.innerHTML = backups.map(backup => `
            <div class="backup-item">
                <div class="backup-info">
                    <i class="fas fa-archive"></i>
                    <div>
                        <div class="backup-name">${escapeHtml(backup.name)}</div>
                        <div class="backup-meta">
                            ${formatFileSize(backup.size)} • ${formatJstDateTime(backup.created)}
                        </div>
                    </div>
                </div>
            </div>
        `).join('');
    } catch (error) {
        backupList.innerHTML = `<div class="alert alert-error">${error.message}</div>`;
    }
}

async function createBackup(containerId) {
    try {
        const result = await api(`/api/containers/${containerId}/backup`, { method: 'POST' });
        showToast('バックアップを作成しました', 'success');
        loadBackups(containerId);
    } catch (error) {
        showToast(error.message, 'error');
    }
}

// ============ Admin ============
function formatComplianceStatusLabel(status) {
    return COMPLIANCE_STATUS_LABELS[status] || status || '不明';
}

function formatComplianceSeverityLabel(severity) {
    return COMPLIANCE_SEVERITY_LABELS[severity] || severity || '中';
}

function complianceStatusClassName(status) {
    if (status === 'investigating') return 'investigating';
    if (status === 'resolved') return 'resolved';
    if (status === 'ignored') return 'ignored';
    return 'open';
}

function complianceSeverityClassName(severity) {
    if (severity === 'critical') return 'critical';
    if (severity === 'high') return 'high';
    if (severity === 'medium') return 'medium';
    return 'low';
}

function parseComplianceEvidence(row) {
    try {
        const parsed = JSON.parse(row.evidence_json || '{}');
        if (parsed && typeof parsed === 'object') return parsed;
    } catch (e) {}
    return {};
}

function formatComplianceEvidence(row) {
    const evidence = parseComplianceEvidence(row);
    if (row.rule_key === 'login_ip_spread_24h') {
        return `24hログイン${evidence.loginCount || 0}件 / IP${evidence.uniqueIpCount || 0}種`;
    }
    if (row.rule_key === 'container_action_burst_10m') {
        return `10分操作${evidence.actionCount || 0}件 / 対象${evidence.uniqueContainerCount || 0}台`;
    }
    if (row.rule_key === 'support_ticket_spam_30m') {
        return `30分の新規問い合わせ${evidence.ticketCount || 0}件`;
    }
    if (row.rule_key === 'support_reply_spam_10m') {
        return `10分の返信${evidence.replyCount || 0}件`;
    }
    return '-';
}

function renderComplianceSummaryCards() {
    const summary = complianceSummary || {};
    document.getElementById('compliance-open-count')?.replaceChildren(document.createTextNode(String(summary.open || 0)));
    document.getElementById('compliance-investigating-count')?.replaceChildren(document.createTextNode(String(summary.investigating || 0)));
    document.getElementById('compliance-high-count')?.replaceChildren(document.createTextNode(String(summary.high_open || 0)));
    document.getElementById('compliance-detected24-count')?.replaceChildren(document.createTextNode(String(summary.detected_24h || 0)));
}

function renderComplianceIncidentRows() {
    const tbody = document.getElementById('compliance-incident-rows');
    if (!tbody) return;

    const filtered = complianceStatusFilter === 'all'
        ? complianceIncidents
        : complianceIncidents.filter((row) => String(row.status) === complianceStatusFilter);

    if (!filtered.length) {
        tbody.innerHTML = `
            <tr>
                <td colspan="8" class="text-muted" style="text-align:center; padding: 24px;">
                    条件に一致するインシデントはありません。
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = filtered.map((row) => {
        const statusClass = complianceStatusClassName(row.status);
        const severityClass = complianceSeverityClassName(row.severity);
        const userLabel = row.user_email
            ? `${escapeHtml(row.user_name || '-')}\n(${escapeHtml(row.user_email)})`
            : '-';
        const detectedAt = row.last_detected_at ? formatJstDateTime(row.last_detected_at) : '-';
        const occurrenceCount = Number(row.occurrence_count || 0);
        const note = row.resolution_note ? `<div class="compliance-note">${escapeHtml(row.resolution_note)}</div>` : '';

        let actionHtml = '';
        if (row.status === 'open') {
            actionHtml = `
                <button class="btn btn-secondary btn-small" onclick="updateComplianceIncidentStatus('${escapeHtml(row.id)}', 'investigating')">調査中</button>
                <button class="btn btn-primary btn-small" onclick="updateComplianceIncidentStatus('${escapeHtml(row.id)}', 'resolved')">解決済み</button>
                <button class="btn btn-danger btn-small" onclick="updateComplianceIncidentStatus('${escapeHtml(row.id)}', 'ignored')">誤検知</button>
            `;
        } else if (row.status === 'investigating') {
            actionHtml = `
                <button class="btn btn-primary btn-small" onclick="updateComplianceIncidentStatus('${escapeHtml(row.id)}', 'resolved')">解決済み</button>
                <button class="btn btn-danger btn-small" onclick="updateComplianceIncidentStatus('${escapeHtml(row.id)}', 'ignored')">誤検知</button>
                <button class="btn btn-secondary btn-small" onclick="updateComplianceIncidentStatus('${escapeHtml(row.id)}', 'open')">未対応へ戻す</button>
            `;
        } else {
            actionHtml = `
                <button class="btn btn-secondary btn-small" onclick="updateComplianceIncidentStatus('${escapeHtml(row.id)}', 'open')">再オープン</button>
            `;
        }

        return `
            <tr>
                <td><code>${escapeHtml(row.id || '-')}</code></td>
                <td>${escapeHtml(row.rule_key || '-')}</td>
                <td>${userLabel.replace('\n', '<br>')}</td>
                <td>
                    <span class="compliance-severity-badge ${severityClass}">
                        ${escapeHtml(formatComplianceSeverityLabel(row.severity))}
                    </span>
                </td>
                <td>
                    <span class="compliance-status-badge ${statusClass}">
                        ${escapeHtml(formatComplianceStatusLabel(row.status))}
                    </span>
                </td>
                <td>
                    <div>${escapeHtml(formatComplianceEvidence(row))}</div>
                    <div class="text-muted" style="font-size: 12px;">発生回数: ${occurrenceCount}</div>
                    ${note}
                </td>
                <td>${escapeHtml(detectedAt)}</td>
                <td>
                    <div class="compliance-actions">${actionHtml}</div>
                </td>
            </tr>
        `;
    }).join('');
}

function setComplianceStatusFilter(status) {
    complianceStatusFilter = String(status || 'open').trim().toLowerCase() || 'open';
    renderComplianceIncidentRows();
}

async function refreshComplianceData() {
    const [summary, incidents] = await Promise.all([
        api('/api/admin/compliance/summary'),
        api('/api/admin/compliance/incidents?status=all&limit=500')
    ]);

    complianceSummary = summary || {};
    complianceIncidents = Array.isArray(incidents) ? incidents : [];
    renderComplianceSummaryCards();
    renderComplianceIncidentRows();
}

async function runComplianceScan() {
    const btn = document.getElementById('compliance-scan-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> スキャン中...';
    }

    try {
        const result = await api('/api/admin/compliance/scan', { method: 'POST' });
        showToast(`再スキャン完了: ${result.scanned_users || 0}ユーザー`, 'success');
        await refreshComplianceData();
    } catch (error) {
        showToast(error.message, 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-shield-alt"></i> 今すぐ再スキャン';
        }
    }
}

async function updateComplianceIncidentStatus(incidentId, status) {
    const id = String(incidentId || '').trim();
    const nextStatus = String(status || '').trim().toLowerCase();
    if (!id || !nextStatus) return;

    let note = '';
    if (nextStatus === 'resolved' || nextStatus === 'ignored') {
        note = window.prompt('対応メモ（任意）', '') || '';
    }

    try {
        await api(`/api/admin/compliance/incidents/${encodeURIComponent(id)}/status`, {
            method: 'PUT',
            body: JSON.stringify({ status: nextStatus, note })
        });
        showToast('インシデントを更新しました', 'success');
        await refreshComplianceData();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function loadAdminContent(tab) {
    const content = document.getElementById('admin-content');
    content.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    
    try {
        if (tab === 'users') {
            const users = await api('/api/admin/users');
            content.innerHTML = `
                <table class="admin-table">
                    <thead>
                        <tr>
                            <th>名前</th>
                            <th>メール</th>
                            <th>プラン</th>
                            <th>管理者</th>
                            <th>登録日</th>
                            <th>操作</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${users.map(u => `
                            <tr>
                                <td>${escapeHtml(u.name)}</td>
                                <td>${escapeHtml(u.email)}</td>
                                <td><span class="plan-badge plan-${u.plan}">${u.plan}</span></td>
                                <td>${u.is_admin ? '<i class="fas fa-check text-green"></i>' : ''}</td>
                                <td>${formatJstDate(u.created_at)}</td>
                                <td>
                                    <select class="form-select form-select-small" onchange="updateUserPlan('${u.id}', this.value)">
                                        <option value="free" ${u.plan === 'free' ? 'selected' : ''}>Free</option>
                                        <option value="standard" ${u.plan === 'standard' ? 'selected' : ''}>Standard</option>
                                        <option value="pro" ${u.plan === 'pro' ? 'selected' : ''}>Pro</option>
                                    </select>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            `;
        } else if (tab === 'containers') {
            const allContainers = await api('/api/admin/containers');
            content.innerHTML = `
                <table class="admin-table">
                    <thead>
                        <tr>
                            <th>名前</th>
                            <th>ユーザー</th>
                            <th>タイプ</th>
                            <th>状態</th>
                            <th>プラン</th>
                            <th>作成日</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${allContainers.map(c => `
                            <tr>
                                <td>${escapeHtml(c.name)}</td>
                                <td>${escapeHtml(c.user_email)}</td>
                                <td>${c.type}</td>
                                <td><span class="status-badge ${c.status}">${c.status}</span></td>
                                <td><span class="plan-badge plan-${c.plan}">${c.plan}</span></td>
                                <td>${formatJstDate(c.created_at)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            `;
        } else if (tab === 'logs') {
            const logs = await api('/api/admin/logs');
            content.innerHTML = `
                <table class="admin-table">
                    <thead>
                        <tr>
                            <th>日時</th>
                            <th>アクション</th>
                            <th>ユーザーID</th>
                            <th>コンテナID</th>
                            <th>IP</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${logs.map(log => `
                            <tr>
                                <td>${formatJstDateTime(log.created_at)}</td>
                                <td>${log.action}</td>
                                <td>${log.user_id || '-'}</td>
                                <td>${log.container_id || '-'}</td>
                                <td>${log.ip_address || '-'}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            `;
        } else if (tab === 'compliance') {
            content.innerHTML = `
                <div class="compliance-toolbar">
                    <div class="compliance-summary-grid">
                        <div class="compliance-summary-card">
                            <div class="label">未対応</div>
                            <div class="value" id="compliance-open-count">0</div>
                        </div>
                        <div class="compliance-summary-card">
                            <div class="label">調査中</div>
                            <div class="value" id="compliance-investigating-count">0</div>
                        </div>
                        <div class="compliance-summary-card">
                            <div class="label">高優先(未対応)</div>
                            <div class="value" id="compliance-high-count">0</div>
                        </div>
                        <div class="compliance-summary-card">
                            <div class="label">24h検出件数</div>
                            <div class="value" id="compliance-detected24-count">0</div>
                        </div>
                    </div>
                    <div class="compliance-toolbar-actions">
                        <select class="form-select form-select-small" id="compliance-status-filter" onchange="setComplianceStatusFilter(this.value)">
                            <option value="open">未対応のみ</option>
                            <option value="investigating">調査中のみ</option>
                            <option value="all">すべて</option>
                            <option value="resolved">解決済みのみ</option>
                            <option value="ignored">誤検知/除外のみ</option>
                        </select>
                        <button class="btn btn-secondary btn-small" onclick="refreshComplianceData()">
                            <i class="fas fa-sync-alt"></i> 更新
                        </button>
                        <button class="btn btn-primary btn-small" id="compliance-scan-btn" onclick="runComplianceScan()">
                            <i class="fas fa-shield-alt"></i> 今すぐ再スキャン
                        </button>
                    </div>
                </div>
                <div style="margin-top: 14px;">
                    <table class="admin-table">
                        <thead>
                            <tr>
                                <th>インシデントID</th>
                                <th>ルール</th>
                                <th>ユーザー</th>
                                <th>重大度</th>
                                <th>ステータス</th>
                                <th>根拠</th>
                                <th>最終検知</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody id="compliance-incident-rows">
                            <tr><td colspan="8"><div class="loading"><div class="spinner"></div></div></td></tr>
                        </tbody>
                    </table>
                </div>
            `;
            complianceStatusFilter = 'open';
            const filterEl = document.getElementById('compliance-status-filter');
            if (filterEl) filterEl.value = complianceStatusFilter;
            await refreshComplianceData();
        } else if (tab === 'support') {
            content.innerHTML = `
                <div class="support-card">
                    <h3><i class="fas fa-life-ring"></i> 問い合わせ管理</h3>
                    <p class="text-muted" style="margin-top: 10px; line-height: 1.8;">
                        問い合わせ対応は専用チャットへ移行しました。<br>
                        管理者は <code>/chat/admin</code> を利用してください。
                    </p>
                    <div style="margin-top: 14px; display:flex; gap:10px; flex-wrap:wrap;">
                        <a class="btn btn-primary" href="/chat/admin">
                            <i class="fas fa-arrow-up-right-from-square"></i> 管理者チャットを開く
                        </a>
                    </div>
                </div>
            `;
        }
    } catch (error) {
        content.innerHTML = `<div class="alert alert-error">${error.message}</div>`;
    }
}

async function updateUserPlan(userId, plan) {
    try {
        await api(`/api/admin/users/${userId}/plan`, {
            method: 'PUT',
            body: JSON.stringify({ plan })
        });
        showToast('プランを更新しました', 'success');
    } catch (error) {
        showToast(error.message, 'error');
    }
}

// ============ Modals ============
function openCreateModal() {
    document.getElementById('create-modal').classList.add('active');
}

function closeCreateModal() {
    document.getElementById('create-modal').classList.remove('active');
    document.getElementById('create-server-form').reset();
}

function closeConfirmModal() {
    document.getElementById('confirm-modal').classList.remove('active');
}

function closeEditorModal() {
    document.getElementById('editor-modal').classList.remove('active');
}

// ============ Navigation ============
function navigateTo(page, options = {}) {
    showPage(page, options);
}

// showPage - navigateToのエイリアス（既に定義済みのため削除）
// showPage関数はファイル上部で定義されています

// ============ Event Listeners ============
document.querySelectorAll('.sidebar-nav a').forEach(link => {
    link.addEventListener('click', (e) => {
        const page = link.dataset.page;
        if (!page) {
            return;
        }
        e.preventDefault();
        navigateTo(page);
        
        if (page === 'admin' && user.is_admin) {
            loadAdminContent('users');
        }
    });
});

document.querySelectorAll('.admin-tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.admin-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        loadAdminContent(tab.dataset.tab);
    });
});

document.getElementById('create-server-btn')?.addEventListener('click', openCreateModal);
document.getElementById('upgrade-plan-btn')?.addEventListener('click', startUpgradeCheckout);
document.getElementById('manage-subscription-btn')?.addEventListener('click', openBillingPortal);
document.getElementById('reapply-plan-limits-btn')?.addEventListener('click', reapplyPlanLimits);

document.getElementById('create-server-form')?.addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const name = document.getElementById('server-name').value;
    const type = document.getElementById('server-type').value || 'ubuntu';
    const plan = document.getElementById('server-plan').value;
    const autoRestart = document.getElementById('auto-restart').checked;
    const startupCommand = document.getElementById('startup-command')?.value || '';
    
    const submitBtn = e.target.querySelector('button[type="submit"]');
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 作成中...';
    
    try {
        await api('/api/containers', {
            method: 'POST',
            body: JSON.stringify({ 
                name, 
                type, 
                plan, 
                auto_restart: autoRestart,
                startup_command: startupCommand
            })
        });
        
        closeCreateModal();
        showToast('サーバーを作成しました！', 'success');
        await loadContainers();
        await loadStats();
    } catch (error) {
        showToast(error.message, 'error');
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = '<i class="fas fa-plus"></i> 作成';
    }
});

document.getElementById('console-server-select')?.addEventListener('change', (e) => {
    loadLogs(e.target.value);
});

document.getElementById('refresh-logs-btn')?.addEventListener('click', () => {
    const id = document.getElementById('console-server-select').value;
    if (id) loadLogs(id);
});

document.getElementById('clear-console-btn')?.addEventListener('click', () => {
    document.getElementById('console-output').innerHTML = '';
});

document.getElementById('file-server-select')?.addEventListener('change', (e) => {
    if (e.target.value) {
        selectedContainerId = e.target.value;
        currentPath = '/app';
        loadFiles(e.target.value, '/app');
    }
});

document.getElementById('file-upload')?.addEventListener('change', async (e) => {
    if (!selectedContainerId || !e.target.files.length) return;
    
    const formData = new FormData();
    Array.from(e.target.files).forEach(file => {
        formData.append('files', file);
    });
    
    try {
        const response = await fetch(`${API_URL}/api/containers/${selectedContainerId}/files/upload?path=${encodeURIComponent(currentPath || '/app')}`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}` },
            body: formData
        });
        
        const data = await response.json();
        if (!response.ok) throw new Error(data.error);
        
        showToast(data.message, 'success');
        loadFiles(selectedContainerId, currentPath);
    } catch (error) {
        showToast(error.message, 'error');
    }
    
    e.target.value = '';
});

document.getElementById('env-server-select')?.addEventListener('change', (e) => {
    loadEnvVars(e.target.value);
});

document.getElementById('backup-server-select')?.addEventListener('change', (e) => {
    loadBackups(e.target.value);
});

document.getElementById('support-create-form')?.addEventListener('submit', createSupportTicket);
document.getElementById('refresh-support-btn')?.addEventListener('click', () => loadSupportTickets(true));
document.getElementById('support-send-reply-btn')?.addEventListener('click', sendSupportReply);
document.getElementById('support-toggle-status-btn')?.addEventListener('click', toggleSupportTicketStatus);
document.getElementById('support-reply-input')?.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        sendSupportReply();
    }
});

document.getElementById('logout-btn').addEventListener('click', () => {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    window.location.href = '/auth.html';
});

// モーダル外クリックで閉じる
document.querySelectorAll('.modal-overlay').forEach(overlay => {
    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) {
            overlay.classList.remove('active');
        }
    });
});

// ============ Utilities ============
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

async function loadPatchNotes() {
    const list = document.getElementById('patches-list');
    if (!list) return;

    list.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    try {
        const response = await fetch(`${API_URL}/patches.json?ts=${Date.now()}`, { cache: 'no-store' });
        if (!response.ok) {
            throw new Error('パッチ履歴の取得に失敗しました');
        }

        const payload = await response.json();
        const patches = Array.isArray(payload) ? payload : (payload.patches || []);

        if (!patches.length) {
            list.innerHTML = '<div class="text-muted text-center" style="padding:2rem;">表示できるパッチ履歴がありません</div>';
            return;
        }

        list.innerHTML = patches.map((patch) => {
            const date = escapeHtml(String(patch.date || '-'));
            const title = escapeHtml(String(patch.title || '更新'));
            const summary = escapeHtml(String(patch.summary || ''));
            const commit = patch.commit ? `<code style="font-size:12px;">${escapeHtml(String(patch.commit))}</code>` : '';
            const type = escapeHtml(String(patch.type || 'update'));
            return `
                <div class="server-card">
                    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">
                        <h3 style="margin:0;">${title}</h3>
                        <span class="status-badge status-running">${type}</span>
                    </div>
                    <p style="margin:10px 0 12px; color: var(--text-secondary);">${summary}</p>
                    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">
                        <small class="text-muted">${date}</small>
                        ${commit}
                    </div>
                </div>
            `;
        }).join('');
    } catch (error) {
        list.innerHTML = `<div class="text-center text-muted" style="padding:2rem;">${escapeHtml(error.message)}</div>`;
    }
}

// ============ Support ============
function formatSupportCategoryLabel(category) {
    return SUPPORT_CATEGORY_LABELS[category] || category || '未分類';
}

function formatSupportStatusLabel(status) {
    return SUPPORT_STATUS_LABELS[status] || status || '不明';
}

function supportStatusClassName(status) {
    const value = String(status || '').toLowerCase();
    return value.replace(/[^a-z_]/g, '') || 'open';
}

function renderSupportStatusBadge(status) {
    const css = supportStatusClassName(status);
    return `<span class="support-status-badge status-${css}">${escapeHtml(formatSupportStatusLabel(status))}</span>`;
}

function truncateText(text, length = 90) {
    const value = String(text || '').trim();
    if (value.length <= length) return value;
    return `${value.slice(0, length - 1)}…`;
}

function renderSupportMessageTimeline(messages, { mode = 'user', ticketOwnerName = 'ユーザー' } = {}) {
    if (!Array.isArray(messages) || messages.length === 0) {
        return '<div class="text-muted text-center" style="padding: 2rem;">まだメッセージがありません</div>';
    }
    return messages.map((msg) => {
        const role = msg.sender_role === 'admin' ? 'admin' : 'user';
        const senderLabel = role === 'admin'
            ? (msg.user_id === user.id ? '運営 (あなた)' : '運営')
            : (mode === 'admin' ? (msg.user_name || ticketOwnerName || 'ユーザー') : 'あなた');
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

function renderSupportTicketList() {
    const listEl = document.getElementById('support-ticket-list');
    const countEl = document.getElementById('support-ticket-count');
    if (!listEl) return;

    if (countEl) {
        countEl.textContent = `${supportTickets.length}件`;
    }

    if (!supportTickets.length) {
        listEl.innerHTML = `
            <div class="text-muted text-center" style="padding: 2rem;">
                問い合わせ履歴はまだありません
            </div>
        `;
        return;
    }

    listEl.innerHTML = supportTickets.map((ticket) => {
        const isActive = ticket.id === activeSupportTicketId;
        const preview = truncateText(ticket.last_message || '');
        return `
            <button type="button" class="support-ticket-item ${isActive ? 'active' : ''}" onclick="selectSupportTicket('${ticket.id}')">
                <div class="support-ticket-item-head">
                    <div class="support-ticket-item-title">${escapeHtml(ticket.subject)}</div>
                    ${renderSupportStatusBadge(ticket.status)}
                </div>
                <div class="support-ticket-item-meta">
                    <span class="support-category-badge ${escapeHtml(ticket.category)}">${escapeHtml(formatSupportCategoryLabel(ticket.category))}</span>
                    <small class="text-muted">${formatJstDateTime(ticket.last_message_at || ticket.updated_at || ticket.created_at)}</small>
                </div>
                <div class="support-ticket-item-preview">${escapeHtml(preview || '（本文なし）')}</div>
            </button>
        `;
    }).join('');
}

function resetSupportThread() {
    const titleEl = document.getElementById('support-thread-title');
    const metaEl = document.getElementById('support-thread-meta');
    const statusEl = document.getElementById('support-thread-status');
    const messagesEl = document.getElementById('support-thread-messages');
    const replyInput = document.getElementById('support-reply-input');
    const sendBtn = document.getElementById('support-send-reply-btn');
    const toggleBtn = document.getElementById('support-toggle-status-btn');

    if (titleEl) titleEl.innerHTML = '<i class="fas fa-comments"></i> 問い合わせ詳細';
    if (metaEl) metaEl.textContent = '問い合わせを選択してください';
    if (statusEl) {
        statusEl.className = 'support-status-badge status-open';
        statusEl.textContent = '未選択';
    }
    if (messagesEl) {
        messagesEl.innerHTML = '<div class="text-muted text-center" style="padding: 2rem;">左の一覧から問い合わせを選択してください</div>';
    }
    if (replyInput) {
        replyInput.value = '';
        replyInput.disabled = true;
    }
    if (sendBtn) sendBtn.disabled = true;
    if (toggleBtn) {
        toggleBtn.disabled = true;
        toggleBtn.innerHTML = '<i class="fas fa-check"></i> 完了にする';
    }
}

async function loadSupportTickets(keepSelection = true) {
    const listEl = document.getElementById('support-ticket-list');
    if (!listEl) return;

    listEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    try {
        supportTickets = await api('/api/support/tickets');
        renderSupportTicketList();

        if (!supportTickets.length) {
            activeSupportTicketId = null;
            resetSupportThread();
            return;
        }

        let nextId = activeSupportTicketId;
        if (!keepSelection || !supportTickets.some((ticket) => ticket.id === nextId)) {
            nextId = supportTickets[0].id;
        }
        if (nextId) {
            await selectSupportTicket(nextId, false);
        }
    } catch (error) {
        listEl.innerHTML = `<div class="alert alert-error">${escapeHtml(error.message)}</div>`;
        resetSupportThread();
    }
}

function getSupportToggleStatus(ticket) {
    if (!ticket) return 'closed';
    return ticket.status === 'closed' ? 'waiting_admin' : 'closed';
}

function getSupportToggleLabel(ticket) {
    if (!ticket) return '完了にする';
    return ticket.status === 'closed' ? '再オープン' : '完了にする';
}

async function selectSupportTicket(ticketId, rerenderList = true) {
    activeSupportTicketId = ticketId;
    if (rerenderList) renderSupportTicketList();

    try {
        const data = await api(`/api/support/tickets/${ticketId}/messages`);
        const ticket = data.ticket;
        const messages = Array.isArray(data.messages) ? data.messages : [];
        supportTickets = supportTickets.map((item) => (item.id === ticket.id ? { ...item, ...ticket } : item));

        const titleEl = document.getElementById('support-thread-title');
        const metaEl = document.getElementById('support-thread-meta');
        const statusEl = document.getElementById('support-thread-status');
        const messagesEl = document.getElementById('support-thread-messages');
        const replyInput = document.getElementById('support-reply-input');
        const sendBtn = document.getElementById('support-send-reply-btn');
        const toggleBtn = document.getElementById('support-toggle-status-btn');

        if (titleEl) {
            titleEl.innerHTML = `<i class="fas fa-comments"></i> ${escapeHtml(ticket.subject)}`;
        }
        if (metaEl) {
            metaEl.textContent = `カテゴリ: ${formatSupportCategoryLabel(ticket.category)} ・ 作成: ${formatJstDateTime(ticket.created_at)}`;
        }
        if (statusEl) {
            statusEl.className = `support-status-badge status-${supportStatusClassName(ticket.status)}`;
            statusEl.textContent = formatSupportStatusLabel(ticket.status);
        }
        if (messagesEl) {
            messagesEl.innerHTML = renderSupportMessageTimeline(messages, { mode: 'user', ticketOwnerName: ticket.user_name });
            messagesEl.scrollTop = messagesEl.scrollHeight;
        }

        const closed = ticket.status === 'closed';
        if (replyInput) {
            replyInput.disabled = closed;
            replyInput.placeholder = closed ? '完了済みのため返信できません。再オープンしてください。' : '返信を入力...';
        }
        if (sendBtn) sendBtn.disabled = closed;
        if (toggleBtn) {
            const nextStatus = getSupportToggleStatus(ticket);
            toggleBtn.disabled = false;
            toggleBtn.dataset.nextStatus = nextStatus;
            toggleBtn.innerHTML = ticket.status === 'closed'
                ? '<i class="fas fa-rotate-left"></i> 再オープン'
                : '<i class="fas fa-check"></i> 完了にする';
        }
        renderSupportTicketList();
    } catch (error) {
        showToast(`問い合わせの読み込みに失敗しました: ${error.message}`, 'error');
        resetSupportThread();
    }
}

async function createSupportTicket(event) {
    event.preventDefault();
    const categoryEl = document.getElementById('support-category');
    const subjectEl = document.getElementById('support-subject');
    const messageEl = document.getElementById('support-message');
    const submitBtn = event.target.querySelector('button[type="submit"]');

    const category = categoryEl?.value || 'inquiry';
    const subject = subjectEl?.value?.trim() || '';
    const message = messageEl?.value?.trim() || '';

    if (subject.length < 2) {
        showToast('件名を入力してください', 'warning');
        return;
    }
    if (message.length < 2) {
        showToast('本文を入力してください', 'warning');
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
        await loadSupportTickets(false);
        if (result.id) {
            await selectSupportTicket(result.id);
        }
    } catch (error) {
        showToast(`送信に失敗しました: ${error.message}`, 'error');
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.innerHTML = '<i class="fas fa-paper-plane"></i> 送信';
        }
    }
}

async function sendSupportReply() {
    if (!activeSupportTicketId) return;
    const inputEl = document.getElementById('support-reply-input');
    const sendBtn = document.getElementById('support-send-reply-btn');
    const message = inputEl?.value?.trim() || '';
    if (!message) {
        showToast('返信内容を入力してください', 'warning');
        return;
    }

    if (sendBtn) {
        sendBtn.disabled = true;
        sendBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 送信中...';
    }
    try {
        await api(`/api/support/tickets/${activeSupportTicketId}/replies`, {
            method: 'POST',
            body: JSON.stringify({ message })
        });
        if (inputEl) inputEl.value = '';
        showToast('返信を送信しました', 'success');
        await loadSupportTickets(true);
    } catch (error) {
        showToast(`返信に失敗しました: ${error.message}`, 'error');
    } finally {
        if (sendBtn) {
            sendBtn.disabled = false;
            sendBtn.innerHTML = '<i class="fas fa-reply"></i> 返信を送信';
        }
    }
}

async function toggleSupportTicketStatus() {
    if (!activeSupportTicketId) return;
    const ticket = supportTickets.find((item) => item.id === activeSupportTicketId);
    const nextStatus = getSupportToggleStatus(ticket);
    const toggleBtn = document.getElementById('support-toggle-status-btn');

    if (toggleBtn) {
        toggleBtn.disabled = true;
    }
    try {
        await api(`/api/support/tickets/${activeSupportTicketId}/status`, {
            method: 'PUT',
            body: JSON.stringify({ status: nextStatus })
        });
        showToast(nextStatus === 'closed' ? '問い合わせを完了にしました' : '問い合わせを再オープンしました', 'success');
        await loadSupportTickets(true);
    } catch (error) {
        showToast(`ステータス更新に失敗しました: ${error.message}`, 'error');
    } finally {
        if (toggleBtn) {
            toggleBtn.disabled = false;
        }
    }
}

async function loadAdminSupportTickets(keepSelection = true) {
    const listEl = document.getElementById('admin-support-ticket-list');
    if (!listEl) return;

    listEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    try {
        adminSupportTickets = await api('/api/support/tickets?scope=all');
        renderAdminSupportTicketList();

        if (!adminSupportTickets.length) {
            activeAdminSupportTicketId = null;
            const threadEl = document.getElementById('admin-support-thread');
            if (threadEl) {
                threadEl.innerHTML = '<div class="text-muted text-center" style="padding: 2rem;">問い合わせはありません</div>';
            }
            return;
        }

        let nextId = activeAdminSupportTicketId;
        if (!keepSelection || !adminSupportTickets.some((ticket) => ticket.id === nextId)) {
            nextId = adminSupportTickets[0].id;
        }
        if (nextId) {
            await openAdminSupportTicket(nextId, false);
        }
    } catch (error) {
        listEl.innerHTML = `<div class="alert alert-error">${escapeHtml(error.message)}</div>`;
    }
}

function renderAdminSupportTicketList() {
    const listEl = document.getElementById('admin-support-ticket-list');
    if (!listEl) return;

    if (!adminSupportTickets.length) {
        listEl.innerHTML = '<div class="text-muted text-center" style="padding: 1.5rem;">問い合わせはありません</div>';
        return;
    }

    listEl.innerHTML = adminSupportTickets.map((ticket) => {
        const isActive = ticket.id === activeAdminSupportTicketId;
        return `
            <button type="button" class="support-ticket-item ${isActive ? 'active' : ''}" onclick="openAdminSupportTicket('${ticket.id}')">
                <div class="support-ticket-item-head">
                    <div class="support-ticket-item-title">${escapeHtml(ticket.subject)}</div>
                    ${renderSupportStatusBadge(ticket.status)}
                </div>
                <div class="support-ticket-item-meta">
                    <span class="support-category-badge ${escapeHtml(ticket.category)}">${escapeHtml(formatSupportCategoryLabel(ticket.category))}</span>
                    <small class="text-muted">${escapeHtml(ticket.user_name || ticket.user_email || '-')}</small>
                </div>
                <div class="support-ticket-item-preview">${escapeHtml(truncateText(ticket.last_message || ''))}</div>
            </button>
        `;
    }).join('');
}

async function openAdminSupportTicket(ticketId, rerenderList = true) {
    activeAdminSupportTicketId = ticketId;
    if (rerenderList) renderAdminSupportTicketList();

    const threadEl = document.getElementById('admin-support-thread');
    if (!threadEl) return;

    threadEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    try {
        const data = await api(`/api/support/tickets/${ticketId}/messages`);
        const ticket = data.ticket;
        const messages = Array.isArray(data.messages) ? data.messages : [];
        adminSupportTickets = adminSupportTickets.map((item) => (item.id === ticket.id ? { ...item, ...ticket } : item));

        threadEl.innerHTML = `
            <div class="support-thread-header">
                <div>
                    <h3 style="margin: 0 0 0.35rem;">${escapeHtml(ticket.subject)}</h3>
                    <div class="support-thread-meta">
                        ${escapeHtml(ticket.user_name || ticket.user_email || ticket.user_id)} / ${escapeHtml(formatSupportCategoryLabel(ticket.category))} / ${formatJstDateTime(ticket.created_at)}
                    </div>
                </div>
                ${renderSupportStatusBadge(ticket.status)}
            </div>
            <div class="support-admin-controls">
                <select class="form-select" id="admin-support-status">
                    <option value="waiting_admin" ${ticket.status === 'waiting_admin' ? 'selected' : ''}>運営対応待ち</option>
                    <option value="waiting_user" ${ticket.status === 'waiting_user' ? 'selected' : ''}>ユーザー返信待ち</option>
                    <option value="open" ${ticket.status === 'open' ? 'selected' : ''}>受付中</option>
                    <option value="closed" ${ticket.status === 'closed' ? 'selected' : ''}>完了</option>
                </select>
                <button class="btn btn-secondary btn-small" onclick="updateAdminSupportStatus()">
                    <i class="fas fa-check"></i> 状態更新
                </button>
            </div>
            <div class="support-thread-messages" id="admin-support-messages">
                ${renderSupportMessageTimeline(messages, { mode: 'admin', ticketOwnerName: ticket.user_name })}
            </div>
            <div class="support-reply-box">
                <textarea class="form-input" id="admin-support-reply-input" rows="4" maxlength="4000" placeholder="運営返信を入力..."></textarea>
                <div class="support-reply-actions">
                    <span class="text-muted">返信送信でステータスは「ユーザー返信待ち」になります</span>
                    <button class="btn btn-primary" onclick="sendAdminSupportReply()">
                        <i class="fas fa-reply"></i> 返信送信
                    </button>
                </div>
            </div>
        `;

        const msgEl = document.getElementById('admin-support-messages');
        if (msgEl) msgEl.scrollTop = msgEl.scrollHeight;
        renderAdminSupportTicketList();
    } catch (error) {
        threadEl.innerHTML = `<div class="alert alert-error">${escapeHtml(error.message)}</div>`;
    }
}

async function updateAdminSupportStatus() {
    if (!activeAdminSupportTicketId) return;
    const statusEl = document.getElementById('admin-support-status');
    const status = statusEl?.value;
    if (!status) return;

    try {
        await api(`/api/support/tickets/${activeAdminSupportTicketId}/status`, {
            method: 'PUT',
            body: JSON.stringify({ status })
        });
        showToast('問い合わせ状態を更新しました', 'success');
        await loadAdminSupportTickets(true);
    } catch (error) {
        showToast(`状態更新に失敗しました: ${error.message}`, 'error');
    }
}

async function sendAdminSupportReply() {
    if (!activeAdminSupportTicketId) return;
    const inputEl = document.getElementById('admin-support-reply-input');
    const message = inputEl?.value?.trim() || '';
    if (!message) {
        showToast('返信内容を入力してください', 'warning');
        return;
    }

    try {
        await api(`/api/support/tickets/${activeAdminSupportTicketId}/replies`, {
            method: 'POST',
            body: JSON.stringify({ message })
        });
        showToast('返信を送信しました', 'success');
        if (inputEl) inputEl.value = '';
        await loadAdminSupportTickets(true);
    } catch (error) {
        showToast(`返信に失敗しました: ${error.message}`, 'error');
    }
}

// ============ Console Tab Switching ============
document.querySelectorAll('.console-tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.console-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        
        const tabName = tab.dataset.tab;
        document.getElementById('terminal-container').classList.toggle('hidden', tabName !== 'terminal');
        document.getElementById('logs-container').classList.toggle('hidden', tabName !== 'logs');
        document.getElementById('command-container').classList.toggle('hidden', tabName !== 'command');
    });
});

// ============ Terminal Input ============
document.getElementById('terminal-input')?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        const input = e.target.value.trim();
        if (input) {
            sendTerminalInput(input);
            e.target.value = '';
        }
    }
});

// ============ Connect Terminal Button ============
document.getElementById('connect-terminal-btn')?.addEventListener('click', () => {
    const containerId = document.getElementById('console-server-select').value;
    if (containerId) {
        connectTerminal(containerId);
    }
});

document.getElementById('console-server-select')?.addEventListener('change', (e) => {
    document.getElementById('connect-terminal-btn').disabled = !e.target.value;
    if (!e.target.value) {
        disconnectTerminal();
    }
    // ログも自動ロード
    if (e.target.value) {
        loadLogs(e.target.value);
    }
});

// ============ Command Execution ============
document.getElementById('exec-command-btn')?.addEventListener('click', async () => {
    const containerId = document.getElementById('console-server-select').value;
    const command = document.getElementById('exec-command').value.trim();
    
    if (!containerId) {
        showToast('サーバーを選択してください', 'warning');
        return;
    }
    if (!command) {
        showToast('コマンドを入力してください', 'warning');
        return;
    }
    
    const outputDiv = document.getElementById('command-output');
    outputDiv.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    
    try {
        const result = await api(`/api/containers/${containerId}/exec`, {
            method: 'POST',
            body: JSON.stringify({ command })
        });
        
        outputDiv.innerHTML = `<pre class="command-result">${escapeHtml(result.output || 'コマンドが実行されました')}</pre>`;
    } catch (error) {
        outputDiv.innerHTML = `<div class="error">${error.message}</div>`;
    }
});

document.getElementById('exec-command')?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        document.getElementById('exec-command-btn').click();
    }
});

// ============ Initialize ============
async function initializeDashboard() {
    await loadUserInfo();
    await loadStats();
    await loadContainers();
    await loadServerTypes();
    connectWebSocket();
    await handleBillingRedirect();
    applyPendingRouteState({ syncUrl: false, replaceHistory: true, showMissingToast: false });
}

initializeDashboard().catch((error) => {
    console.error('Dashboard initialization error:', error);
});

// 定期更新（30秒ごと）
setInterval(() => {
    loadStats();
    loadContainers();
}, 30000);

// ============ Server Detail Page ============
function openServerDetail(serverId, options = {}) {
    const {
        syncUrl = true,
        replaceHistory = false
    } = options;
    const server = containers.find(c => c.id === serverId);
    if (!server) {
        showToast('サーバーが見つかりません', 'error');
        return;
    }
    
    currentDetailServer = server;
    detailContainerId = serverId;
    detailCurrentPath = '/app';
    
    updateServerDetailUI(server);
    showPage('server-detail', { syncUrl, replaceHistory, serverId });
    
    // ファイル一覧を読み込み
    detailLoadFiles();
    
    // 稼働中なら自動でターミナル接続
    if (server.status === 'running') {
        setTimeout(() => detailConnectTerminal(), 500);
    }
}

// ターミナルクリア
function clearDetailTerminal() {
    const output = document.getElementById('detail-terminal-output');
    if (output) {
        output.innerHTML = '<div class="terminal-welcome">ターミナルをクリアしました</div>';
    }
}

// ターミナル入力送信
function sendDetailTerminalInput() {
    const input = document.getElementById('detail-terminal-input');
    if (input && detailTerminalWs && detailTerminalWs.readyState === WebSocket.OPEN) {
        const text = input.value;
        detailTerminalWs.send(JSON.stringify({ type: 'input', data: text + '\n' }));
        input.value = '';
    }
}

// 特殊キー送信
function sendDetailKey(key) {
    if (detailTerminalWs && detailTerminalWs.readyState === WebSocket.OPEN) {
        detailTerminalWs.send(JSON.stringify({ type: 'input', data: key }));
    }
}

// Ctrl+キー送信
function sendDetailTerminalCtrl() {
    const input = document.getElementById('detail-terminal-input');
    if (input && input.value && detailTerminalWs && detailTerminalWs.readyState === WebSocket.OPEN) {
        const char = input.value.toLowerCase().charCodeAt(0);
        if (char >= 97 && char <= 122) { // a-z
            const ctrlCode = String.fromCharCode(char - 96);
            detailTerminalWs.send(JSON.stringify({ type: 'input', data: ctrlCode }));
            input.value = '';
        }
    }
}

async function generateDetailTtydTunnel() {
    if (!currentDetailServer) {
        showToast('サーバーが選択されていません', 'warning');
        return;
    }
    if (getBackendType(currentDetailServer) !== 'vm') {
        showToast('この機能はVMバックエンド専用です', 'warning');
        return;
    }

    const btn = document.getElementById('detail-generate-ttyd-btn');
    const urlEl = document.getElementById('detail-ttyd-url');
    const authEl = document.getElementById('detail-ttyd-auth');
    const openBtn = document.getElementById('detail-open-ttyd-btn');
    const original = btn ? btn.innerHTML : '';

    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 準備中...';
    }

    try {
        const result = await api(`/api/containers/${currentDetailServer.id}/vm/ttyd-tunnel`, {
            method: 'POST',
            body: JSON.stringify({})
        });
        detailTtydUrl = result.url || '';

        if (urlEl) urlEl.textContent = detailTtydUrl || '取得失敗';
        if (authEl) {
            authEl.textContent = '不要（自動認証）';
        }
        if (openBtn) openBtn.disabled = !detailTtydUrl;
        showToast('固定URLの接続先を更新しました', 'success');
    } catch (error) {
        showToast(`リンク発行に失敗しました: ${error.message}`, 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = original || '<i class="fas fa-bolt"></i> 接続を再準備';
        }
    }
}

function openDetailTtydUrl() {
    if (!detailTtydUrl) {
        showToast('先にリンクを発行してください', 'warning');
        return;
    }
    window.open(detailTtydUrl, '_blank', 'noopener,noreferrer');
}

function updateServerDetailUI(server) {
    const backendType = getBackendType(server);
    const isVmBackend = backendType === 'vm';

    // ヘッダー情報
    document.getElementById('detail-server-name').textContent = server.name;
    
    // ステータス表示
    const statusEl = document.getElementById('detail-server-status');
    const isRunning = server.status === 'running';
    statusEl.className = `detail-status ${isRunning ? '' : 'stopped'}`;
    statusEl.querySelector('.status-text').textContent = isRunning ? '実行中' : '停止中';
    
    // 最終更新時刻
    const updatedEl = document.getElementById('detail-updated');
    if (updatedEl) {
        const date = new Date(server.updated_at);
        updatedEl.textContent = `最終更新: ${formatJstDate(date)} ${formatJstTime(date, { hour: '2-digit', minute: '2-digit', second: '2-digit' })}`;
    }
    
    // サイドバー: アドレス
    const addressEl = document.getElementById('detail-ssh-address');
    if (addressEl) {
        addressEl.textContent = server.ssh_host ? `${server.ssh_host}:${server.ssh_port || 4104}` : '-';
    }
    
    // サイドバー: パスワード
    const passwordEl = document.getElementById('detail-ssh-password');
    if (passwordEl) {
        passwordEl.textContent = '●●●●●●●●';
        passwordEl.dataset.password = server.ssh_password || '';
    }
    
    // ネットワークタブ内のパスワード
    const passwordNetworkEl = document.getElementById('detail-ssh-password-network');
    if (passwordNetworkEl) {
        passwordNetworkEl.textContent = '••••••••';
        passwordNetworkEl.dataset.password = server.ssh_password || '';
    }
    
    // サイドバー: ユーザー名
    const userDisplayEl = document.getElementById('detail-ssh-user-display');
    if (userDisplayEl) {
        userDisplayEl.textContent = server.ssh_user || '-';
    }
    
    // サイドバー: サーバー情報
    const backendTypeEl = document.getElementById('detail-backend-type');
    if (backendTypeEl) backendTypeEl.textContent = isVmBackend ? 'VPSモード (KVM/libvirt)' : 'Dockerコンテナ';

    const specSourceEl = document.getElementById('detail-spec-source');
    if (specSourceEl) specSourceEl.textContent = server.resource_source === 'vm' ? '実機スペック（適用中）' : '設定スペック';

    const cpuSpecEl = document.getElementById('detail-cpu-spec');
    if (cpuSpecEl) cpuSpecEl.textContent = formatCpuLimit(server.cpu_limit || 0.5);
    
    const memorySpecEl = document.getElementById('detail-memory-spec');
    if (memorySpecEl) memorySpecEl.textContent = formatResourceMB(server.memory_limit || 512);
    
    const storageSpecEl = document.getElementById('detail-storage-spec');
    if (storageSpecEl) storageSpecEl.textContent = formatResourceMB(server.storage_limit || 1024);

    const specNoteRow = document.getElementById('detail-spec-note-row');
    const specNoteEl = document.getElementById('detail-spec-note');
    const configuredCpuLimit = server.configured_cpu_limit ?? server.cpu_limit;
    const configuredMemoryLimit = server.configured_memory_limit ?? server.memory_limit;
    const configuredStorageLimit = server.configured_storage_limit ?? server.storage_limit;
    const hasSpecDiff = isVmBackend && hasVmSpecDiff(server);
    if (specNoteRow && specNoteEl) {
        if (hasSpecDiff) {
            specNoteRow.style.display = '';
            specNoteEl.textContent = `${formatCpuLimit(configuredCpuLimit)} / ${formatResourceMB(configuredMemoryLimit)} / ${formatResourceMB(configuredStorageLimit)}（未適用）`;
        } else {
            specNoteRow.style.display = 'none';
            specNoteEl.textContent = '-';
        }
    }
    
    // ディスク使用量
    const diskUsageEl = document.getElementById('detail-disk-usage');
    if (diskUsageEl) {
        const diskUsage = server.stats?.diskUsage || '-';
        const diskPercent = server.stats?.diskUsagePercent;
        diskUsageEl.textContent = diskPercent == null ? diskUsage : `${diskUsage} (${formatPercent(diskPercent)}%)`;
    }
    
    const diskProgressEl = document.getElementById('detail-disk-progress');
    if (diskProgressEl) {
        const diskPercent = server.stats?.diskUsagePercent == null ? 0 : parseFloat(server.stats.diskUsagePercent);
        diskProgressEl.style.width = `${Math.min(diskPercent || 0, 100)}%`;
        diskProgressEl.className = 'progress';
        if (server.stats?.diskUsagePercent == null) {
            diskProgressEl.classList.add('idle');
        } else if (diskPercent > 80) {
            diskProgressEl.classList.add('danger');
        } else if (diskPercent > 60) {
            diskProgressEl.classList.add('warning');
        }
    }
    
    // 有効期限
    const expiryEl = document.getElementById('detail-expiry');
    if (expiryEl) expiryEl.textContent = '-';
    
    // ネットワーク統計 (ネットワークタブ用)
    const networkInEl = document.getElementById('detail-network-in');
    if (networkInEl) networkInEl.textContent = server.stats?.networkIn || '-';
    
    const networkOutEl = document.getElementById('detail-network-out');
    if (networkOutEl) networkOutEl.textContent = server.stats?.networkOut || '-';
    
    // ネットワークタブ: SSH情報
    const sshCommandEl = document.getElementById('detail-ssh-command');
    if (sshCommandEl) sshCommandEl.textContent = server.ssh_command || '-';
    
    const sshHostEl = document.getElementById('detail-ssh-host');
    if (sshHostEl) sshHostEl.textContent = server.ssh_host || '-';
    
    const sshUserEl = document.getElementById('detail-ssh-user');
    if (sshUserEl) sshUserEl.textContent = server.ssh_user || '-';
    
    const sshPortEl = document.getElementById('detail-ssh-port');
    if (sshPortEl) sshPortEl.textContent = server.ssh_port || 4104;

    const ttydSectionEl = document.getElementById('detail-ttyd-section');
    const ttydUrlEl = document.getElementById('detail-ttyd-url');
    const ttydAuthEl = document.getElementById('detail-ttyd-auth');
    const ttydOpenBtn = document.getElementById('detail-open-ttyd-btn');
    if (ttydSectionEl) ttydSectionEl.style.display = isVmBackend ? '' : 'none';
    detailTtydUrl = isVmBackend ? (server.vm_ttyd_url || '') : '';
    if (ttydUrlEl) ttydUrlEl.textContent = isVmBackend ? (detailTtydUrl || '未発行') : 'VM専用機能';
    if (ttydAuthEl) {
        ttydAuthEl.textContent = isVmBackend ? '不要（自動認証）' : '-';
    }
    if (ttydOpenBtn) ttydOpenBtn.disabled = !detailTtydUrl;
    
    // 設定タブ
    const startupCommandEl = document.getElementById('detail-startup-command');
    if (startupCommandEl) startupCommandEl.value = server.startup_command || '';
    
    // 環境変数をテキスト形式に変換
    const envVarsEl = document.getElementById('detail-env-vars');
    if (envVarsEl) {
        const envVars = server.env_vars || {};
        const envText = Object.entries(envVars).map(([k, v]) => `${k}=${v}`).join('\n');
        envVarsEl.value = envText;
    }
    
    const autoRestartEl = document.getElementById('detail-auto-restart');
    if (autoRestartEl) autoRestartEl.checked = server.auto_restart === 1;
}

// 詳細ページ用タブ切り替え
document.querySelectorAll('.detail-tab').forEach(tab => {
    tab.addEventListener('click', () => {
        const tabId = tab.dataset.tab;
        
        // タブのアクティブ状態を更新
        document.querySelectorAll('.detail-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        
        // コンテンツの表示を切り替え
        document.querySelectorAll('.detail-tab-content').forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(tabId).classList.add('active');
    });
});

// 詳細ページ用ターミナル接続
function detailConnectTerminal() {
    if (!currentDetailServer) {
        console.error('No server selected');
        return;
    }
    
    if (detailTerminalWs) {
        detailTerminalWs.close();
    }
    
    const wsUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws?token=${token}&container=${currentDetailServer.id}`;
    console.log('Connecting to WebSocket:', wsUrl);
    
    const terminalOutput = document.getElementById('detail-terminal-output');
    const terminalInput = document.getElementById('detail-terminal-input');
    
    if (!terminalOutput || !terminalInput) {
        console.error('Terminal elements not found');
        return;
    }

    terminalOutput.innerHTML = '<div class="terminal-welcome">接続中...</div>';
    
    try {
        detailTerminalWs = new WebSocket(wsUrl);
    } catch (e) {
        console.error('WebSocket creation error:', e);
        terminalOutput.innerHTML = '<div class="terminal-error">WebSocket接続エラー: ' + e.message + '</div>';
        return;
    }
    
    detailTerminalWs.onopen = () => {
        console.log('WebSocket connected');
        terminalInput.disabled = false;
        terminalInput.focus();
        terminalOutput.innerHTML = '';
    };
    
    detailTerminalWs.onclose = (e) => {
        console.log('WebSocket closed:', e.code, e.reason);
        terminalInput.disabled = true;
        detailAppendTerminalLine('system', '接続が切断されました');
    };
    
    detailTerminalWs.onerror = (e) => {
        console.error('WebSocket error:', e);
        detailAppendTerminalLine('error', 'エラー: 接続に失敗しました');
    };
    
    detailTerminalWs.onmessage = (event) => {
        try {
            if (event.data.startsWith('<!DOCTYPE') || event.data.startsWith('<html')) {
                detailAppendTerminalLine('error', 'サーバーエラー: 接続を再試行してください');
                return;
            }
            
            const data = JSON.parse(event.data);
            
            switch (data.type) {
                case 'connected':
                    detailAppendTerminalLine('success', data.message);
                    break;
                case 'output':
                    detailAppendTerminalOutput(data.data);
                    break;
                case 'error':
                    if (data.action === 'restart_required') {
                        detailAppendTerminalLine('error', data.message);
                        detailAppendRestartButton();
                    } else {
                        detailAppendTerminalLine('error', data.message);
                    }
                    break;
                case 'info':
                    detailAppendTerminalLine('info', data.message);
                    break;
                case 'disconnect':
                    detailAppendTerminalLine('warning', data.message);
                    break;
            }
        } catch (e) {
            if (event.data && !event.data.includes('<!DOCTYPE')) {
                detailAppendTerminalOutput(event.data);
            }
        }
    };
}

function detailAppendTerminalLine(type, text) {
    const output = document.getElementById('detail-terminal-output');
    const line = document.createElement('div');
    line.className = `terminal-line terminal-${type}`;
    line.textContent = text;
    output.appendChild(line);
    output.scrollTop = output.scrollHeight;
}

function detailAppendTerminalOutput(text) {
    const output = document.getElementById('detail-terminal-output');
    let cleanText = text
        .replace(/\x1b\[[0-9;]*m/g, '')
        .replace(/\x1b\[[0-9;]*[ABCDHJ]/g, '')
        .replace(/\x1b\][0-9];[^\x07]*\x07/g, '')
        .replace(/\x1b\[\?[0-9;]*[hl]/g, '')
        .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '')
        .replace(/\r/g, '');
    
    const lines = cleanText.split('\n');
    lines.forEach(line => {
        const trimmedLine = line.trim();
        if (trimmedLine) {
            const lineDiv = document.createElement('div');
            lineDiv.className = 'terminal-line';
            if (trimmedLine.match(/^(root|user)@[^:]+:[^#$]*[#$]\s*$/)) {
                lineDiv.className += ' terminal-prompt';
            }
            lineDiv.textContent = trimmedLine;
            output.appendChild(lineDiv);
        }
    });
    output.scrollTop = output.scrollHeight;
}

function detailAppendRestartButton() {
    const output = document.getElementById('detail-terminal-output');
    const btnContainer = document.createElement('div');
    btnContainer.className = 'terminal-action';
    btnContainer.style.cssText = 'margin: 15px 0; padding: 15px; background: rgba(255,165,0,0.1); border-radius: 8px; text-align: center;';
    
    const btn = document.createElement('button');
    btn.className = 'btn btn-warning';
    btn.innerHTML = '<i class="fas fa-play"></i> コンテナを再作成して起動';
    btn.onclick = async () => {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 起動中...';
        
        try {
            const result = await api(`/api/containers/${currentDetailServer.id}/start`, { method: 'POST' });
            if (result.success) {
                showToast('サーバーを起動しました', 'success');
                btnContainer.innerHTML = '<span style="color: #4caf50;"><i class="fas fa-check"></i> 起動完了！</span>';
                setTimeout(() => {
                    loadContainers();
                    detailConnectTerminal();
                }, 2000);
            }
        } catch (error) {
            showToast('起動に失敗しました: ' + error.message, 'error');
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-play"></i> コンテナを再作成して起動';
        }
    };
    
    btnContainer.appendChild(btn);
    output.appendChild(btnContainer);
    output.scrollTop = output.scrollHeight;
}

// 詳細ページ用ターミナル入力
document.getElementById('detail-terminal-input')?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && detailTerminalWs && detailTerminalWs.readyState === WebSocket.OPEN) {
        const input = e.target.value;
        detailTerminalWs.send(JSON.stringify({ type: 'input', data: input + '\n' }));
        e.target.value = '';
    }
});

// 詳細ページ用サーバー操作
async function detailStartServer() {
    if (!currentDetailServer) return;
    
    try {
        const result = await api(`/api/containers/${currentDetailServer.id}/start`, { method: 'POST' });
        showToast(result.message, 'success');
        await loadContainers();
        currentDetailServer = containers.find(c => c.id === currentDetailServer.id);
        updateServerDetailUI(currentDetailServer);
    } catch (error) {
        showToast('起動に失敗しました: ' + error.message, 'error');
    }
}

async function detailStopServer() {
    if (!currentDetailServer) return;
    
    try {
        const result = await api(`/api/containers/${currentDetailServer.id}/stop`, { method: 'POST' });
        showToast(result.message, 'success');
        await loadContainers();
        currentDetailServer = containers.find(c => c.id === currentDetailServer.id);
        updateServerDetailUI(currentDetailServer);
    } catch (error) {
        showToast('停止に失敗しました: ' + error.message, 'error');
    }
}

async function detailRestartServer() {
    if (!currentDetailServer) return;
    
    try {
        const result = await api(`/api/containers/${currentDetailServer.id}/restart`, { method: 'POST' });
        showToast(result.message, 'success');
        await loadContainers();
        currentDetailServer = containers.find(c => c.id === currentDetailServer.id);
        updateServerDetailUI(currentDetailServer);
    } catch (error) {
        showToast('再起動に失敗しました: ' + error.message, 'error');
    }
}

async function detailDeleteServer() {
    if (!currentDetailServer) return;
    
    if (!confirm(`サーバー「${currentDetailServer.name}」を削除しますか？この操作は取り消せません。`)) {
        return;
    }
    
    try {
        await api(`/api/containers/${currentDetailServer.id}`, { method: 'DELETE' });
        showToast('サーバーを削除しました', 'success');
        showPage('servers');
        loadContainers();
    } catch (error) {
        showToast('削除に失敗しました: ' + error.message, 'error');
    }
}

// 詳細ページ用設定保存
async function detailSaveStartupCommand() {
    if (!currentDetailServer) return;
    
    const command = document.getElementById('detail-startup-command').value;
    
    try {
        await api(`/api/containers/${currentDetailServer.id}/startup`, {
            method: 'PUT',
            body: JSON.stringify({ startup_command: command })
        });
        showToast('起動コマンドを保存しました', 'success');
    } catch (error) {
        showToast('保存に失敗しました: ' + error.message, 'error');
    }
}

async function detailSaveEnvVars() {
    if (!currentDetailServer) return;
    
    const envText = document.getElementById('detail-env-vars').value;
    const envVars = {};
    
    envText.split('\n').forEach(line => {
        const [key, ...valueParts] = line.split('=');
        if (key && valueParts.length > 0) {
            envVars[key.trim()] = valueParts.join('=').trim();
        }
    });
    
    try {
        await api(`/api/containers/${currentDetailServer.id}/env`, {
            method: 'PUT',
            body: JSON.stringify({ env_vars: envVars })
        });
        showToast('環境変数を保存しました。反映には再起動が必要です。', 'success');
    } catch (error) {
        showToast('保存に失敗しました: ' + error.message, 'error');
    }
}

// サーバー詳細画面用の状態
let detailCurrentPath = '/app';
let detailContainerId = null;

// ファイルアップロード（詳細画面）
function detailUploadFile() {
    if (!detailContainerId) {
        showToast('サーバーが選択されていません', 'error');
        return;
    }
    document.getElementById('detail-file-upload').click();
}

// ファイルアップロード処理
document.getElementById('detail-file-upload')?.addEventListener('change', async (e) => {
    if (!detailContainerId || !e.target.files.length) return;
    
    const formData = new FormData();
    Array.from(e.target.files).forEach(file => {
        formData.append('files', file);
    });
    
    try {
        showToast('アップロード中...', 'info');
        const response = await fetch(`${API_URL}/api/containers/${detailContainerId}/files/upload?path=${encodeURIComponent(detailCurrentPath)}`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}` },
            body: formData
        });
        
        const data = await response.json();
        if (!response.ok) throw new Error(data.error);
        
        showToast(data.message, 'success');
        detailLoadFiles();
    } catch (error) {
        showToast('アップロードに失敗しました: ' + error.message, 'error');
    }
    
    e.target.value = '';
});

// フォルダ作成モーダルを開く
function detailCreateFolder() {
    if (!detailContainerId) {
        showToast('サーバーが選択されていません', 'error');
        return;
    }
    document.getElementById('create-folder-modal').classList.add('active');
    document.getElementById('new-folder-name').value = '';
    document.getElementById('new-folder-name').focus();
}

// フォルダ作成モーダルを閉じる
function closeCreateFolderModal() {
    document.getElementById('create-folder-modal').classList.remove('active');
}

// フォルダ作成を実行
async function submitCreateFolder(e) {
    e.preventDefault();
    
    const folderName = document.getElementById('new-folder-name').value.trim();
    if (!folderName) {
        showToast('フォルダ名を入力してください', 'error');
        return;
    }
    
    const fullPath = detailCurrentPath.endsWith('/') 
        ? detailCurrentPath + folderName 
        : detailCurrentPath + '/' + folderName;
    
    try {
        await api(`/api/containers/${detailContainerId}/files/mkdir`, {
            method: 'POST',
            body: JSON.stringify({ path: fullPath })
        });
        
        showToast('フォルダを作成しました', 'success');
        closeCreateFolderModal();
        detailLoadFiles();
    } catch (error) {
        showToast('フォルダの作成に失敗しました: ' + error.message, 'error');
    }
}

// ファイル一覧を更新（詳細画面）
function detailRefreshFiles() {
    detailLoadFiles();
}

// ファイル一覧を読み込み（詳細画面）
async function detailLoadFiles() {
    if (!detailContainerId) return;
    
    const fileListEl = document.getElementById('detail-file-list');
    const pathEl = document.getElementById('detail-current-path');
    if (pathEl) pathEl.textContent = detailCurrentPath;
    
    try {
        const data = await api(`/api/containers/${detailContainerId}/files?path=${encodeURIComponent(detailCurrentPath)}`);
        
        if (!data.items || data.items.length === 0) {
            fileListEl.innerHTML = `
                <div class="text-muted text-center" style="padding: 2rem;">
                    <i class="fas fa-folder-open" style="font-size: 2rem; opacity: 0.5;"></i>
                    <p style="margin-top: 0.5rem;">このフォルダは空です</p>
                </div>
            `;
            return;
        }
        
        // ソート: ディレクトリ優先、次に名前順
        const sortedItems = data.items.sort((a, b) => {
            if (a.type === 'directory' && b.type !== 'directory') return -1;
            if (a.type !== 'directory' && b.type === 'directory') return 1;
            return a.name.localeCompare(b.name);
        });
        
        let html = '';
        
        // 親ディレクトリに戻るボタン
        if (detailCurrentPath !== '/app' && detailCurrentPath !== '/') {
            html += `
                <div class="file-item file-item-directory" onclick="detailNavigateUp()">
                    <div class="file-icon"><i class="fas fa-level-up-alt"></i></div>
                    <div class="file-name">..</div>
                    <div class="file-size">-</div>
                    <div class="file-actions"></div>
                </div>
            `;
        }
        
        for (const item of sortedItems) {
            const icon = item.type === 'directory' ? 'fa-folder' : getFileIcon(item.name);
            const size = item.type === 'directory' ? '-' : formatFileSize(item.size);
            
            if (item.type === 'directory') {
                html += `
                    <div class="file-item file-item-directory" onclick="detailNavigateTo('${escapeHtml(item.name)}')">
                        <div class="file-icon"><i class="fas ${icon}"></i></div>
                        <div class="file-name">${escapeHtml(item.name)}</div>
                        <div class="file-size">${size}</div>
                        <div class="file-actions">
                            <button class="btn btn-sm btn-danger" onclick="event.stopPropagation(); detailDeleteFile('${escapeHtml(item.name)}', true)" title="削除">
                                <i class="fas fa-trash"></i>
                            </button>
                        </div>
                    </div>
                `;
            } else {
                html += `
                    <div class="file-item" onclick="detailOpenFile('${escapeHtml(item.name)}')">
                        <div class="file-icon"><i class="fas ${icon}"></i></div>
                        <div class="file-name">${escapeHtml(item.name)}</div>
                        <div class="file-size">${size}</div>
                        <div class="file-actions">
                            <button class="btn btn-sm btn-danger" onclick="event.stopPropagation(); detailDeleteFile('${escapeHtml(item.name)}', false)" title="削除">
                                <i class="fas fa-trash"></i>
                            </button>
                        </div>
                    </div>
                `;
            }
        }
        
        fileListEl.innerHTML = html;
    } catch (error) {
        fileListEl.innerHTML = `
            <div class="text-muted text-center" style="padding: 2rem; color: #ff6b6b;">
                <i class="fas fa-exclamation-circle" style="font-size: 2rem;"></i>
                <p style="margin-top: 0.5rem;">ファイル一覧の取得に失敗しました</p>
                <p style="font-size: 0.8rem;">${escapeHtml(error.message)}</p>
            </div>
        `;
    }
}

// ディレクトリに移動
function detailNavigateTo(name) {
    detailCurrentPath = detailCurrentPath.endsWith('/') 
        ? detailCurrentPath + name 
        : detailCurrentPath + '/' + name;
    detailLoadFiles();
}

// 親ディレクトリに戻る
function detailNavigateUp() {
    const parts = detailCurrentPath.split('/').filter(p => p);
    parts.pop();
    detailCurrentPath = '/' + parts.join('/') || '/app';
    detailLoadFiles();
}

// ファイルを開く（内容を表示/編集）
async function detailOpenFile(name) {
    const filePath = detailCurrentPath.endsWith('/') 
        ? detailCurrentPath + name 
        : detailCurrentPath + '/' + name;
    
    try {
        const data = await api(`/api/containers/${detailContainerId}/files/content?path=${encodeURIComponent(filePath)}`);
        
        // 簡易エディタをモーダルで表示
        showFileEditor(filePath, data.content);
    } catch (error) {
        showToast('ファイルを開けませんでした: ' + error.message, 'error');
    }
}

// ファイル/フォルダを削除
async function detailDeleteFile(name, isDirectory) {
    const itemType = isDirectory ? 'フォルダ' : 'ファイル';
    if (!confirm(`「${name}」を削除しますか？この操作は取り消せません。`)) return;
    
    const targetPath = detailCurrentPath.endsWith('/') 
        ? detailCurrentPath + name 
        : detailCurrentPath + '/' + name;
    
    try {
        await api(`/api/containers/${detailContainerId}/files`, {
            method: 'DELETE',
            body: JSON.stringify({ path: targetPath })
        });
        
        showToast(`${itemType}を削除しました`, 'success');
        detailLoadFiles();
    } catch (error) {
        showToast(`${itemType}の削除に失敗しました: ` + error.message, 'error');
    }
}

// ファイルエディタを表示
function showFileEditor(filePath, content) {
    // 既存のエディタがあれば削除
    const existingEditor = document.getElementById('file-editor-modal');
    if (existingEditor) existingEditor.remove();
    
    const fileName = filePath.split('/').pop();
    
    const modal = document.createElement('div');
    modal.id = 'file-editor-modal';
    modal.className = 'modal-overlay active';
    modal.innerHTML = `
        <div class="modal" style="max-width: 900px; width: 95%;">
            <div class="modal-header">
                <h2 class="modal-title"><i class="fas fa-edit"></i> ${escapeHtml(fileName)}</h2>
                <button class="modal-close" onclick="closeFileEditor()">&times;</button>
            </div>
            <div style="padding: 0;">
                <textarea id="file-editor-content" style="width: 100%; height: 400px; font-family: 'JetBrains Mono', monospace; font-size: 14px; padding: 1rem; border: none; background: #1a1a2e; color: #eee; resize: vertical;">${escapeHtml(content)}</textarea>
            </div>
            <div class="modal-footer">
                <span style="font-size: 0.8rem; color: #888; margin-right: auto;">${escapeHtml(filePath)}</span>
                <button class="btn btn-secondary" onclick="closeFileEditor()">キャンセル</button>
                <button class="btn btn-primary" onclick="saveFileContent('${escapeHtml(filePath)}')">
                    <i class="fas fa-save"></i> 保存
                </button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
}

// ファイルエディタを閉じる
function closeFileEditor() {
    const modal = document.getElementById('file-editor-modal');
    if (modal) modal.remove();
}

// ファイル内容を保存
async function saveFileContent(filePath) {
    const content = document.getElementById('file-editor-content').value;
    
    try {
        await api(`/api/containers/${detailContainerId}/files/content`, {
            method: 'POST',
            body: JSON.stringify({ path: filePath, content: content })
        });
        
        showToast('ファイルを保存しました', 'success');
        closeFileEditor();
    } catch (error) {
        showToast('保存に失敗しました: ' + error.message, 'error');
    }
}

// ファイルサイズをフォーマット
function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// ファイル拡張子に応じたアイコンを取得
function getFileIcon(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    const icons = {
        'js': 'fa-file-code',
        'ts': 'fa-file-code',
        'py': 'fa-file-code',
        'java': 'fa-file-code',
        'go': 'fa-file-code',
        'html': 'fa-file-code',
        'css': 'fa-file-code',
        'json': 'fa-file-code',
        'xml': 'fa-file-code',
        'md': 'fa-file-alt',
        'txt': 'fa-file-alt',
        'log': 'fa-file-alt',
        'sh': 'fa-terminal',
        'bash': 'fa-terminal',
        'png': 'fa-file-image',
        'jpg': 'fa-file-image',
        'jpeg': 'fa-file-image',
        'gif': 'fa-file-image',
        'svg': 'fa-file-image',
        'pdf': 'fa-file-pdf',
        'zip': 'fa-file-archive',
        'tar': 'fa-file-archive',
        'gz': 'fa-file-archive',
    };
    return icons[ext] || 'fa-file';
}

// ============ SSH公開鍵管理 ============

// SSH鍵一覧を読み込み
async function loadSshKeys() {
    const listEl = document.getElementById('ssh-keys-list');
    if (!listEl) return;
    
    try {
        const keys = await api('/api/ssh-keys');
        
        if (keys.length === 0) {
            listEl.innerHTML = `
                <div class="ssh-keys-empty">
                    <i class="fas fa-key"></i>
                    <p>SSH公開鍵が登録されていません</p>
                    <button class="btn btn-primary btn-small" onclick="openAddSshKeyModal()">
                        <i class="fas fa-plus"></i> 鍵を追加
                    </button>
                </div>
            `;
            return;
        }
        
        listEl.innerHTML = keys.map(key => `
            <div class="ssh-key-item">
                <div class="ssh-key-info">
                    <div class="ssh-key-name"><i class="fas fa-key"></i> ${escapeHtml(key.name)}</div>
                    <div class="ssh-key-fingerprint">${escapeHtml(key.fingerprint)}</div>
                    <div class="ssh-key-date">追加日: ${formatJstDate(key.created_at)}</div>
                </div>
                <div class="ssh-key-actions">
                    <button class="btn btn-danger btn-small" onclick="deleteSshKey(${key.id}, '${escapeHtml(key.name)}')" title="削除">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        `).join('');
    } catch (error) {
        console.error('SSH keys load error:', error);
        listEl.innerHTML = `
            <div class="ssh-keys-empty">
                <i class="fas fa-exclamation-triangle"></i>
                <p>SSH鍵の読み込みに失敗しました</p>
            </div>
        `;
    }
}

// SSH鍵追加モーダルを開く
function openAddSshKeyModal() {
    document.getElementById('ssh-key-modal').classList.add('active');
    document.getElementById('ssh-key-name').value = '';
    document.getElementById('ssh-key-content').value = '';
    document.getElementById('ssh-key-name').focus();
}

// SSH鍵追加モーダルを閉じる
function closeSshKeyModal() {
    document.getElementById('ssh-key-modal').classList.remove('active');
}

// SSH鍵を追加
async function addSshKey(event) {
    event.preventDefault();
    
    const name = document.getElementById('ssh-key-name').value.trim();
    const publicKey = document.getElementById('ssh-key-content').value.trim();
    
    if (!name || !publicKey) {
        showToast('名前と公開鍵を入力してください', 'error');
        return;
    }
    
    try {
        const result = await api('/api/ssh-keys', {
            method: 'POST',
            body: JSON.stringify({ name, public_key: publicKey })
        });
        
        showToast(result.message || 'SSH公開鍵を登録しました', 'success');
        closeSshKeyModal();
        loadSshKeys();
    } catch (error) {
        showToast(error.message || 'SSH鍵の登録に失敗しました', 'error');
    }
}

// SSH鍵を削除
async function deleteSshKey(keyId, keyName) {
    if (!confirm(`SSH鍵「${keyName}」を削除しますか？`)) {
        return;
    }
    
    try {
        await api(`/api/ssh-keys/${keyId}`, {
            method: 'DELETE'
        });
        
        showToast('SSH公開鍵を削除しました', 'success');
        loadSshKeys();
    } catch (error) {
        showToast(error.message || 'SSH鍵の削除に失敗しました', 'error');
    }
}



// ============ セキュリティ設定（パスキー・TOTP） ============

// セキュリティ状態を読み込み
async function loadSecurityStatus() {
    try {
        const status = await api('/api/security/status');
        
        // パスキー状態更新
        const passkeyStatus = document.getElementById('passkey-status');
        const passkeysList = document.getElementById('passkeys-list');
        
        if (status.passkeys && status.passkeys.length > 0) {
            passkeyStatus.textContent = '有効';
            passkeyStatus.className = 'security-badge enabled';
            
            passkeysList.innerHTML = status.passkeys.map(pk => `
                <div class="passkey-item">
                    <div>
                        <div class="passkey-name"><i class="fas fa-key"></i> ${escapeHtml(pk.name)}</div>
                        <div class="passkey-date">登録: ${formatJstDate(pk.created_at)}</div>
                    </div>
                    <button class="btn btn-danger btn-small" onclick="deletePasskey(${pk.id}, '${escapeHtml(pk.name)}')" title="削除">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            `).join('');
        } else {
            passkeyStatus.textContent = '無効';
            passkeyStatus.className = 'security-badge disabled';
            passkeysList.innerHTML = '<p style="color: var(--text-secondary); font-size: 13px;">パスキーが登録されていません</p>';
        }
        
        // TOTP状態更新
        const totpStatus = document.getElementById('totp-status');
        const totpControls = document.getElementById('totp-controls');
        
        if (status.totp_enabled) {
            totpStatus.textContent = '有効';
            totpStatus.className = 'security-badge enabled';
            totpControls.innerHTML = `
                <button class="btn btn-danger btn-small" onclick="disableTOTP()">
                    <i class="fas fa-times"></i> 無効化
                </button>
            `;
        } else {
            totpStatus.textContent = '無効';
            totpStatus.className = 'security-badge disabled';
            totpControls.innerHTML = `
                <button class="btn btn-primary btn-small" onclick="setupTOTP()">
                    <i class="fas fa-cog"></i> 設定する
                </button>
            `;
        }
    } catch (error) {
        console.error('Load security status error:', error);
    }
}

// パスキー登録開始
async function registerPasskey() {
    document.getElementById('passkey-modal').classList.add('active');
    document.getElementById('passkey-name').value = '';
    document.getElementById('passkey-name').focus();
}

function closePasskeyModal() {
    document.getElementById('passkey-modal').classList.remove('active');
}

// パスキー登録実行
async function submitPasskeyRegistration(event) {
    event.preventDefault();
    
    const name = document.getElementById('passkey-name').value.trim();
    if (!name) {
        showToast('パスキーの名前を入力してください', 'error');
        return;
    }
    
    try {
        // 登録オプションを取得
        const options = await api('/api/passkeys/register/options', { method: 'POST' });
        
        // WebAuthn API呼び出し
        const credential = await navigator.credentials.create({
            publicKey: {
                ...options,
                challenge: base64URLToBuffer(options.challenge),
                user: {
                    ...options.user,
                    id: base64URLToBuffer(options.user.id)
                },
                excludeCredentials: options.excludeCredentials?.map(c => ({
                    ...c,
                    id: base64URLToBuffer(c.id)
                })) || []
            }
        });
        
        // 登録を検証
        const response = {
            id: credential.id,
            rawId: bufferToBase64URL(credential.rawId),
            response: {
                clientDataJSON: bufferToBase64URL(credential.response.clientDataJSON),
                attestationObject: bufferToBase64URL(credential.response.attestationObject)
            },
            type: credential.type
        };
        
        await api('/api/passkeys/register/verify', {
            method: 'POST',
            body: JSON.stringify({ name, response })
        });
        
        showToast('パスキーを登録しました', 'success');
        closePasskeyModal();
        loadSecurityStatus();
    } catch (error) {
        if (error.name === 'NotAllowedError') {
            showToast('パスキーの登録がキャンセルされました', 'info');
        } else {
            showToast('パスキーの登録に失敗しました: ' + error.message, 'error');
        }
    }
}

// パスキー削除
async function deletePasskey(id, name) {
    if (!confirm(`パスキー「${name}」を削除しますか？`)) return;
    
    try {
        await api(`/api/passkeys/${id}`, { method: 'DELETE' });
        showToast('パスキーを削除しました', 'success');
        loadSecurityStatus();
    } catch (error) {
        showToast('パスキーの削除に失敗しました: ' + error.message, 'error');
    }
}

// TOTP設定開始
async function setupTOTP() {
    document.getElementById('totp-modal').classList.add('active');
    document.getElementById('totp-setup-content').innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    
    try {
        const data = await api('/api/totp/setup', { method: 'POST' });
        
        document.getElementById('totp-setup-content').innerHTML = `
            <div style="text-align: center; padding: 20px;">
                <p style="margin-bottom: 16px;">認証アプリ（Google Authenticator等）でQRコードをスキャンしてください。</p>
                <img src="${data.qrCode}" alt="QR Code" style="max-width: 200px; margin-bottom: 16px; border-radius: 8px;">
                <p style="font-size: 12px; color: var(--text-secondary); margin-bottom: 16px;">
                    または手動でシークレットを入力: <code style="font-size: 11px;">${data.secret}</code>
                </p>
                <div class="form-group">
                    <label class="form-label">認証コード</label>
                    <input type="text" class="form-input" id="totp-verify-code" placeholder="6桁のコード" maxlength="6" pattern="[0-9]{6}" style="text-align: center; font-size: 18px; letter-spacing: 4px;">
                </div>
                <button class="btn btn-primary" onclick="verifyTOTPSetup()">
                    <i class="fas fa-check"></i> 確認して有効化
                </button>
                
                <div style="margin-top: 20px; padding: 16px; background: var(--bg-secondary); border-radius: 8px; text-align: left;">
                    <h4 style="margin-bottom: 12px;"><i class="fas fa-key"></i> バックアップコード</h4>
                    <p style="font-size: 12px; color: var(--text-secondary); margin-bottom: 12px;">
                        認証アプリにアクセスできない場合に使用できます。安全な場所に保管してください。
                    </p>
                    <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; font-family: monospace; font-size: 13px;">
                        ${data.backupCodes.map(code => `<div style="background: rgba(0,0,0,0.3); padding: 6px; border-radius: 4px;">${code}</div>`).join('')}
                    </div>
                </div>
            </div>
        `;
        
        document.getElementById('totp-verify-code').focus();
    } catch (error) {
        document.getElementById('totp-setup-content').innerHTML = `
            <div style="text-align: center; padding: 20px; color: #ff6b6b;">
                <i class="fas fa-exclamation-circle" style="font-size: 48px; margin-bottom: 16px;"></i>
                <p>${error.message}</p>
            </div>
        `;
    }
}

function closeTOTPModal() {
    document.getElementById('totp-modal').classList.remove('active');
}

// TOTPセットアップ確認
async function verifyTOTPSetup() {
    const code = document.getElementById('totp-verify-code').value.trim();
    
    if (!code || code.length !== 6) {
        showToast('6桁のコードを入力してください', 'error');
        return;
    }
    
    try {
        await api('/api/totp/verify-setup', {
            method: 'POST',
            body: JSON.stringify({ code })
        });
        
        showToast('二段階認証を有効化しました', 'success');
        closeTOTPModal();
        loadSecurityStatus();
    } catch (error) {
        showToast('コードが正しくありません: ' + error.message, 'error');
    }
}

// TOTP無効化
async function disableTOTP() {
    const code = prompt('二段階認証を無効化するには、認証アプリのコードを入力してください:');
    if (!code) return;
    
    try {
        await api('/api/totp/disable', {
            method: 'POST',
            body: JSON.stringify({ code })
        });
        
        showToast('二段階認証を無効化しました', 'success');
        loadSecurityStatus();
    } catch (error) {
        showToast('無効化に失敗しました: ' + error.message, 'error');
    }
}

// Base64URL <-> ArrayBuffer 変換ユーティリティ
function base64URLToBuffer(base64url) {
    if (!base64url) {
        return new Uint8Array().buffer;
    }
    // Handle if it's already a buffer or array
    if (base64url instanceof ArrayBuffer || base64url instanceof Uint8Array) {
        return base64url instanceof Uint8Array ? base64url.buffer : base64url;
    }
    // Ensure it's a string
    const str = String(base64url);
    // Replace base64url characters with base64 characters
    const base64 = str.replace(/-/g, '+').replace(/_/g, '/');
    // Add padding
    const paddingNeeded = (4 - (base64.length % 4)) % 4;
    const paddedBase64 = base64 + '='.repeat(paddingNeeded);
    // Decode
    try {
        const binary = atob(paddedBase64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        return bytes.buffer;
    } catch (e) {
        console.error('base64URLToBuffer decode error:', e, 'input:', base64url);
        throw e;
    }
}

function bufferToBase64URL(buffer) {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

// 設定ページ表示時にセキュリティ状態を読み込み
const originalShowPageForSecurity = showPage;
showPage = function(page, options = {}) {
    originalShowPageForSecurity(page, options);
    if (page === 'settings') {
        loadSecurityStatus();
    }
};
