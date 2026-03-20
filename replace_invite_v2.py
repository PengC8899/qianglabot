import re

with open("/Users/pclucky/私聊/frontend/src/App.jsx", "r") as f:
    content = f.read()

new_invite_manager = """const InviteManager = () => {
  const [groupLink, setGroupLink] = useState('');
  const [targets, setTargets] = useState("");
  const [accounts, setAccounts] = useState([]);
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [joining, setJoining] = useState(false);

  const loadAccounts = async () => {
    try {
      const res = await getInviteAccounts();
      setAccounts(res.items || []);
    } catch (e) {
      console.error(e);
    }
  };

  const loadLogs = async () => {
    try {
      const res = await getInviteLogs();
      setLogs(res.logs || []);
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    loadAccounts();
    loadLogs();
    const interval = setInterval(() => {
      loadAccounts();
      loadLogs();
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleRefresh = async () => {
    if (!groupLink.trim()) {
      message.error("请填写群链接");
      return;
    }
    try {
      setLoading(true);
      message.loading({ content: "正在检测所有账号状态...", key: "refresh", duration: 0 });
      await refreshInviteAccounts(groupLink.trim());
      message.success({ content: "刷新完成", key: "refresh" });
      loadAccounts();
    } catch (e) {
      message.error({ content: "刷新失败: " + e.message, key: "refresh" });
    } finally {
      setLoading(false);
    }
  };

  const handleJoinAll = async () => {
    if (!groupLink.trim()) {
      message.error("请填写群链接");
      return;
    }
    try {
      setJoining(true);
      message.loading({ content: "正在让所有账号进群...", key: "join", duration: 0 });
      await joinAllAccounts(groupLink.trim());
      message.success({ content: "进群任务完成", key: "join" });
      loadAccounts();
    } catch (e) {
      message.error({ content: "进群失败: " + e.message, key: "join" });
    } finally {
      setJoining(false);
    }
  };

  const handleStartInvite = async () => {
    const targetList = targets.split('\\n').map(t => t.trim()).filter(t => t);
    if (!groupLink.trim()) {
      message.error("请填写群链接");
      return;
    }
    if (targetList.length === 0) {
      message.error("请至少输入一个目标用户");
      return;
    }

    const availableAdmins = accounts.filter(a => a.is_admin && a.can_invite);
    if (availableAdmins.length === 0) {
      message.warning("没有可用的管理员账号，请先确保账号进群并拥有拉人权限");
    }

    let queuedCount = 0;
    for (const target of targetList) {
      try {
        await addInviteTask(target, groupLink.trim());
        queuedCount++;
      } catch (e) {
        console.error("Failed to add task for", target, e);
      }
    }
    message.success(`已将 ${queuedCount} 个目标加入邀请队列`);
    setTargets("");
  };

  const accountColumns = [
    { title: "ID", dataIndex: "session_id", key: "session_id", width: 60 },
    { title: "手机号", dataIndex: "phone", key: "phone", width: 120 },
    {
      title: "群内状态",
      key: "status",
      render: (_, r) => {
        if (!r.is_in_group) return <Tag color="default">未进群</Tag>;
        if (r.is_admin) return <Tag color="purple">管理员</Tag>;
        return <Tag color="blue">普通成员</Tag>;
      }
    },
    {
      title: "拉人权限",
      key: "can_invite",
      render: (_, r) => r.can_invite ? <Tag color="green">有</Tag> : <Tag color="red">无</Tag>
    },
    {
      title: "成功/失败",
      key: "stats",
      render: (_, r) => (
        <span>
          <span style={{color: 'green'}}>{r.success_count || 0}</span> / <span style={{color: 'red'}}>{r.fail_count || 0}</span>
        </span>
      )
    },
    { title: "异常", dataIndex: "error", key: "error", ellipsis: true }
  ];

  return (
    <Row gutter={24}>
      <Col span={12}>
        <Card title="1. 目标群组与账号检测">
          <div style={{ marginBottom: 16 }}>
            <div style={{ marginBottom: 4 }}>目标群链接 (例如: https://t.me/xxx):</div>
            <Input
              placeholder="输入群链接"
              value={groupLink}
              onChange={e => setGroupLink(e.target.value)}
            />
          </div>
          <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
            <Button type="default" onClick={handleJoinAll} loading={joining}>
              一键让所有账号进群
            </Button>
            <Button type="primary" onClick={handleRefresh} loading={loading}>
              刷新检测账号状态 (检测管理员与拉人权限)
            </Button>
          </div>
          <Table
            dataSource={accounts}
            columns={accountColumns}
            rowKey="session_id"
            size="small"
            pagination={{ pageSize: 5 }}
          />
        </Card>
        
        <Card title="2. 批量拉人进群" style={{ marginTop: 24 }}>
          <div style={{ marginBottom: 16, padding: 10, borderRadius: 6, background: '#fafafa', border: '1px solid #f0f0f0', color: '#555', fontSize: 12 }}>
            系统会自动使用上述表格中具有“管理员”且“有拉人权限”的账号，轮流去邀请目标。<br/>
            注意：大规模拉人会触发 Telegram 风控，系统已内置了 10~30 秒随机延迟和异常重试机制。
          </div>
          <div style={{ marginBottom: 10 }}>
            <div style={{ marginBottom: 4 }}>邀请目标 (每行一个 @username):</div>
            <TextArea
              rows={8}
              placeholder="@user1\n@user2"
              value={targets}
              onChange={e => setTargets(e.target.value)}
            />
          </div>
          <Button type="primary" block onClick={handleStartInvite} icon={<UsergroupAddOutlined />}>
            开始邀请 (加入队列)
          </Button>
        </Card>
      </Col>
      <Col span={12}>
        <Card title="3. 实时任务执行日志">
          <div style={{
            background: '#1e1e1e',
            color: '#00ff00',
            fontFamily: 'monospace',
            padding: 12,
            borderRadius: 4,
            height: '650px',
            overflowY: 'auto',
            fontSize: 12
          }}>
            {logs.length === 0 ? "暂无日志..." : logs.map((log, i) => (
              <div key={i} style={{ marginBottom: 4, whiteSpace: 'pre-wrap', color: log.includes('失败') ? '#ff4d4f' : '#52c41a' }}>
                {log}
              </div>
            ))}
          </div>
        </Card>
      </Col>
    </Row>
  );
};
"""

# Extract all imports at the top
import_str = """
import { getInviteAccounts, refreshInviteAccounts, addInviteTask, getInviteLogs, joinAllAccounts } from './api';
"""

if "getInviteAccounts" not in content:
    # Insert right after the last import
    last_import_match = re.search(r"import .*?;", content)
    if last_import_match:
        content = content.replace("import './App.css';", "import './App.css';" + import_str)

pattern = re.compile(r"const InviteManager = \(\) => \{.*?(?=const LogStats = \(\) => \{)", re.DOTALL)
new_content = pattern.sub(new_invite_manager + "\n\n", content)

with open("/Users/pclucky/私聊/frontend/src/App.jsx", "w") as f:
    f.write(new_content)
