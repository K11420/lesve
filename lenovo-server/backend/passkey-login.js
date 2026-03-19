// パスキー単独ログインAPI（server.jsにインポートして使用）

export function registerPasskeyLoginRoutes(app, db, jwt, CONFIG, webauthnChallenges, generateAuthenticationOptions, verifyAuthenticationResponse, uuidv4, logActivity) {
    // パスキーログイン開始（認証オプション生成）
    app.post('/api/auth/passkey/start', async (req, res) => {
        try {
            // Discoverable Credentials用（allowCredentials空）
            const options = await generateAuthenticationOptions({
                rpID: CONFIG.RP_ID,
                userVerification: 'preferred',
                allowCredentials: []
            });
            
            const sessionId = uuidv4();
            webauthnChallenges.set(`passkey_login_${sessionId}`, {
                challenge: options.challenge,
                expires: Date.now() + 5 * 60 * 1000
            });
            
            res.json({ ...options, sessionId });
        } catch (e) {
            console.error('Passkey login start error:', e);
            res.status(500).json({ error: 'パスキーログインの開始に失敗しました' });
        }
    });

    // パスキーログイン完了（認証検証）
    app.post('/api/auth/passkey/verify', async (req, res) => {
        try {
            const { sessionId, response } = req.body;
            
            if (!sessionId || !response) {
                return res.status(400).json({ error: 'セッションIDとレスポンスが必要です' });
            }
            
            const challengeData = webauthnChallenges.get(`passkey_login_${sessionId}`);
            if (!challengeData || challengeData.expires < Date.now()) {
                return res.status(400).json({ error: 'セッションが無効または期限切れです' });
            }
            
            const credentialId = response.id;
            const passkey = db.prepare('SELECT * FROM passkeys WHERE credential_id = ?').get(credentialId);
            
            if (!passkey) {
                return res.status(400).json({ error: 'パスキーが見つかりません。先にDiscordでログインしてパスキーを登録してください。' });
            }
            
            const verification = await verifyAuthenticationResponse({
                response,
                expectedChallenge: challengeData.challenge,
                expectedOrigin: CONFIG.RP_ORIGIN,
                expectedRPID: CONFIG.RP_ID,
                authenticator: {
                    credentialID: Buffer.from(passkey.credential_id, 'base64url'),
                    credentialPublicKey: Buffer.from(passkey.public_key, 'base64'),
                    counter: passkey.counter
                }
            });
            
            if (!verification.verified) {
                return res.status(400).json({ error: '認証に失敗しました' });
            }
            
            db.prepare('UPDATE passkeys SET counter = ?, last_used = CURRENT_TIMESTAMP WHERE id = ?')
                .run(verification.authenticationInfo.newCounter, passkey.id);
            
            webauthnChallenges.delete(`passkey_login_${sessionId}`);
            
            const user = db.prepare('SELECT * FROM users WHERE id = ?').get(passkey.user_id);
            if (!user) {
                return res.status(400).json({ error: 'ユーザーが見つかりません' });
            }
            
            const token = jwt.sign(
                { userId: user.id, discordId: user.discord_id }, 
                CONFIG.JWT_SECRET, 
                { expiresIn: '90d' }
            );
            
            logActivity(user.id, null, 'login_passkey', { passkey_name: passkey.name }, req.ip);
            
            res.json({ success: true, token });
        } catch (e) {
            console.error('Passkey login verify error:', e);
            res.status(500).json({ error: 'パスキー認証に失敗しました' });
        }
    });
}
