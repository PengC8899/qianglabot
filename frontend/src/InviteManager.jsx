import React, { useEffect, useState } from 'react';
import { Button, Card, Col, Input, Modal, Popconfirm, Row, Table, Tabs, Tag, message } from 'antd';
import { DeleteOutlined, StopOutlined, SyncOutlined, UsergroupAddOutlined } from '@ant-design/icons';
import { addInviteTask, batchDeleteSessions, clearInviteCooldowns, clearInviteLogs, getInviteAccounts, getInviteLogs, joinAllAccounts, leaveAllAccounts, refreshInviteAccounts, stopInviteTasks } from './api';

const { TextArea } = Input;

const InviteManager = () => {
  const [groupLink, setGroupLink] = useState('');
  const [targets, setTargets] = useState("");
  const [accounts, setAccounts] = useState([]);
  const [logs, setLogs] = useState([]);
  const [stats, setStats] = useState({ success: 0, fail: 0 });
  const [loading, setLoading] = useState(false);
  const [joining, setJoining] = useState(false);
  const [leaving, setLeaving] = useState(false);
  const [clearingCooldown, setClearingCooldown] = useState(false);

  const loadAccounts = async () => {
    try {
      const res = await getInviteAccounts();
      setAccounts(res.items || []);
    } catch (e) {
      return;
    }
  };

  const loadLogs = async () => {
    try {
      const res = await getInviteLogs();
      setLogs(res.logs || []);
      if (res.stats) {
        setStats(res.stats);
      }
    } catch (e) {
      return;
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
      const res = await refreshInviteAccounts(groupLink.trim());
      message.success(res.message || "已在后台开始检测");
    } catch (e) {
      message.error("刷新失败: " + e.message);
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
      const res = await joinAllAccounts(groupLink.trim());
      message.success(res.message || "已在后台开始进群任务");
    } catch (e) {
      message.error("进群失败: " + e.message);
    } finally {
      setJoining(false);
    }
  };

  const handleLeaveAll = async () => {
    if (!groupLink.trim()) {
      message.error("请填写群链接");
      return;
    }
    Modal.confirm({
      title: '确认一键退群',
      content: '这将让所有已在该群内的账号（包括管理员）退出群组，确定继续吗？',
      okText: '确定退出',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          setLeaving(true);
          const res = await leaveAllAccounts(groupLink.trim());
          message.success(res.message || "已在后台开始退群任务");
        } catch (e) {
          message.error("退群失败: " + e.message);
        } finally {
          setLeaving(false);
        }
      }
    });
  };

  const handleStopInvite = async () => {
    try {
      const res = await stopInviteTasks();
      message.success(res.message || "已发送停止指令");
      loadLogs();
    } catch (e) {
      message.error("停止失败: " + e.message);
    }
  };

  const handleClearInviteLogs = async () => {
    try {
      const res = await clearInviteLogs();
      message.success(res.message || "日志已清空");
      setLogs([]);
      setStats({ success: 0, fail: 0 });
    } catch (e) {
      message.error("清空失败: " + e.message);
    }
  };

  const handleClearCooldown = async () => {
    try {
      setClearingCooldown(true);
      const res = await clearInviteCooldowns();
      message.success(res.message || "已清理冷却状态");
      loadAccounts();
      loadLogs();
    } catch (e) {
      message.error("清理冷却失败: " + e.message);
    } finally {
      setClearingCooldown(false);
    }
  };

  const handleStartInvite = async () => {
    const targetList = targets.split('\n').map(t => t.trim()).filter(t => t);
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
        continue;
      }
    }
    message.success(`已将 ${queuedCount} 个目标加入邀请队列`);
    setTargets("");
  };

  const handleDeleteAccount = async (id) => {
    try {
      await batchDeleteSessions([id]);
      message.success("账号已删除");
      loadAccounts();
    } catch (e) {
      message.error("删除失败: " + e.message);
    }
  };

  const accountColumns = [
    {
      title: "序号",
      key: "index",
      width: 60,
      render: (text, record, index) => index + 1
    },
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
      title: "一键加群状态",
      key: "join_status",
      render: (_, r) => {
        if (r.join_status === 'joining') return <Tag color="blue"><SyncOutlined spin /> 进群中...</Tag>;
        if (r.join_status === 'success') return <Tag color="green">进群成功</Tag>;
        if (r.join_status === 'failed') return <Tag color="red">进群失败</Tag>;
        return <Tag color="default">-</Tag>;
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
          <span style={{ color: 'green' }}>{r.success_count || 0}</span> / <span style={{ color: 'red' }}>{r.fail_count || 0}</span>
        </span>
      )
    },
    { title: "异常", dataIndex: "error", key: "error", ellipsis: true },
    {
      title: "操作",
      key: "action",
      render: (_, record) => (
        <Popconfirm title="确定删除该账号吗?" onConfirm={() => handleDeleteAccount(record.session_id)}>
          <Button size="small" icon={<DeleteOutlined />} danger />
        </Popconfirm>
      )
    }
  ];

  const adminAccounts = accounts.filter(a => a.is_admin);

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
            <Button type="default" onClick={handleJoinAll} loading={joining}>一键让所有账号进群</Button>
            <Button type="primary" onClick={handleRefresh} loading={loading}>刷新检测账号状态 (检测管理员与拉人权限)</Button>
            <Button type="dashed" onClick={handleClearCooldown} loading={clearingCooldown}>清掉冷却状态</Button>
            <Button type="default" danger onClick={handleLeaveAll} loading={leaving}>一键退群</Button>
          </div>

          <Tabs defaultActiveKey="1">
            <Tabs.TabPane tab={`全部账号 (${accounts.length})`} key="1">
              <Table
                dataSource={accounts}
                columns={accountColumns}
                rowKey="session_id"
                size="small"
                pagination={{ pageSize: 5 }}
              />
            </Tabs.TabPane>
            <Tabs.TabPane tab={`管理员账号 (${adminAccounts.length})`} key="2">
              <Table
                dataSource={adminAccounts}
                columns={accountColumns}
                rowKey="session_id"
                size="small"
                pagination={{ pageSize: 5 }}
              />
            </Tabs.TabPane>
          </Tabs>
        </Card>

        <Card title="2. 批量拉人进群" style={{ marginTop: 24 }}>
          <div style={{ marginBottom: 16, padding: 10, borderRadius: 6, background: '#fafafa', border: '1px solid #f0f0f0', color: '#555', fontSize: 12 }}>
            系统会自动使用上述表格中具有“管理员”且“有拉人权限”的账号，轮流去邀请目标。<br />
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
          <div style={{ display: 'flex', gap: '8px' }}>
            <Button type="primary" block onClick={handleStartInvite} icon={<UsergroupAddOutlined />}>开始邀请 (加入队列)</Button>
            <Button danger block onClick={handleStopInvite} icon={<StopOutlined />}>停止邀请任务</Button>
          </div>
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
            fontSize: 12,
            marginBottom: 16
          }}>
            {logs.length === 0 ? "暂无日志..." : logs.map((log, i) => (
              <div key={i} style={{ marginBottom: 4, whiteSpace: 'pre-wrap', color: log.includes('失败') ? '#ff4d4f' : '#52c41a' }}>
                {log}
              </div>
            ))}
          </div>
          <div style={{ marginBottom: 12 }}>
            <Button danger onClick={handleClearInviteLogs}>清空日志与计数</Button>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', padding: '0 16px', fontWeight: 'bold', fontSize: 16 }}>
            <span style={{ color: '#52c41a' }}>总成功: {stats.success}</span>
            <span style={{ color: '#ff4d4f' }}>总失败: {stats.fail}</span>
          </div>
        </Card>
      </Col>
    </Row>
  );
};

export default InviteManager;
