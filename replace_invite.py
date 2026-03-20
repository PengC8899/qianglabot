import re

with open("/Users/pclucky/私聊/frontend/src/App.jsx", "r") as f:
    content = f.read()

new_invite_manager = """const InviteManager = () => {
  const [form] = Form.useForm();
  const [groupLink, setGroupLink] = useState('');
  const [targets, setTargets] = useState("");
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [selectedTaskId, setSelectedTaskId] = useState(null);
  const [detailTaskId, setDetailTaskId] = useState(null);
  
  const [accounts, setAccounts] = useState([]);
  const [checkingAccounts, setCheckingAccounts] = useState(false);

  const loadTasks = async () => {
    try {
      setLoading(true);
      const res = await getTasks('invite');
      setTasks(res.items);
    } catch (e) {
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadTasks();
    const interval = setInterval(loadTasks, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleCheckAccounts = async () => {
    if (!groupLink.trim()) {
      message.error("请填写群链接");
      return;
    }
    try {
      setCheckingAccounts(true);
      message.loading({ content: "正在拉取账号在群内的状态...", key: "check_accounts", duration: 0 });
      const res = await checkAccountsInGroup(groupLink.trim());
      if (res.status === "error") {
         message.error({ content: res.message || "拉取失败", key: "check_accounts" });
         return;
      }
      setAccounts(res.items || []);
      message.success({ content: "拉取完成", key: "check_accounts" });
    } catch (e) {
      message.error({ content: "拉取失败: " + e.message, key: "check_accounts" });
    } finally {
      setCheckingAccounts(false);
    }
  };

  const onFinish = async (values) => {
    const targetList = targets.split('\\n').map(t => t.trim()).filter(t => t);
    if (!groupLink.trim()) {
      message.error("请填写群链接");
      return;
    }
    if (targetList.length === 0) {
      message.error("请至少输入一个目标用户");
      return;
    }
    const uniqueTargets = [...new Set(targetList)];
    if (uniqueTargets.length !== targetList.length) {
      message.warning(`检测到重复目标，已自动去重。原: ${targetList.length}, 去重后: ${uniqueTargets.length}`);
    }

    const payload = {
      group_link: groupLink.trim(),
      targets: uniqueTargets,
      delay_seconds: values.delay_seconds,
      random_delay: values.random_delay,
      max_per_account: values.max_per_account
    };

    try {
      await createInviteTask(payload);
      message.success(`邀请任务已创建，共 ${uniqueTargets.length} 个目标`);
      setTargets("");
      loadTasks();
    } catch (e) {
      message.error("邀请任务创建失败: " + e.message);
    }
  };

  const handleFileUpload = (file) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const text = e.target.result;
      setTargets(text);
      message.success(`已加载 ${text.split('\\n').length} 行数据`);
    };
    reader.readAsText(file);
    return false;
  };

  const handleStop = async (id) => {
    try {
      await stopTask(id);
      message.success("任务已停止");
      loadTasks();
    } catch (e) {
      message.error("停止失败");
    }
  };

  const handleRestart = async (id) => {
    try {
      await restartTask(id);
      message.success("任务已重启");
      loadTasks();
    } catch (e) {
      message.error("重启失败: " + e.message);
    }
  };

  const handleDelete = async (id) => {
    try {
      await deleteTask(id);
      message.success("任务已删除");
      if (selectedTaskId === id) setSelectedTaskId(null);
      if (detailTaskId === id) setDetailTaskId(null);
      loadTasks();
    } catch (e) {
      message.error("删除失败");
    }
  };

  const taskColumns = [
    { title: "ID", dataIndex: "id", key: "id", width: 60 },
    { title: "群链接", dataIndex: "group_link", key: "group_link", ellipsis: true, render: t => <Tooltip title={t}>{t || '-'}</Tooltip> },
    {
      title: "进度",
      key: "progress",
      width: 250,
      render: (_, record) => {
        const total = record.total_count || 0;
        const success = record.success_count || 0;
        const failed = record.fail_count || 0;
        const percent = total > 0 ? Math.floor(((success + failed) / total) * 100) : 0;
        return (
          <div>
            <Progress percent={percent} size="small" status={record.status === 'running' ? 'active' : 'normal'} />
            <div style={{ fontSize: 12, display: 'flex', justifyContent: 'space-between' }}>
              <span>总: {total}</span>
              <span style={{ color: '#52c41a' }}>成: {success}</span>
              <span style={{ color: '#ff4d4f' }}>败: {failed}</span>
            </div>
          </div>
        );
      }
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      render: status => {
        let color = status === 'completed' ? 'green' : status === 'failed' ? 'red' : status === 'running' ? 'processing' : 'default';
        return <Tag color={color}>{status}</Tag>;
      }
    },
    { title: "创建时间", dataIndex: "created_at", key: "created_at", render: t => t ? new Date(t).toLocaleString() : '-' },
    {
      title: "操作",
      key: "action",
      width: 220,
      render: (_, record) => (
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
          <Button size="small" type="primary" onClick={() => setDetailTaskId(record.id)}>待处理</Button>
          <Button size="small" onClick={() => setSelectedTaskId(record.id)}>日志</Button>
          {record.status === 'running' && (
            <Popconfirm title="确定停止任务?" onConfirm={() => handleStop(record.id)}>
              <Button size="small" icon={<PauseCircleOutlined />} danger>停止</Button>
            </Popconfirm>
          )}
          {(record.status === 'stopped' || record.status === 'failed' || record.status === 'completed') && (
            <Popconfirm title="确定重启任务? 已成功目标将跳过。" onConfirm={() => handleRestart(record.id)}>
              <Button size="small" type="dashed">重启</Button>
            </Popconfirm>
          )}
          <Popconfirm title="确定删除任务?" description="这将删除相关日志和记录" onConfirm={() => handleDelete(record.id)}>
            <Button size="small" icon={<DeleteOutlined />} danger />
          </Popconfirm>
        </div>
      )
    }
  ];

  const accountColumns = [
    { title: "账号ID", dataIndex: "session_id", key: "session_id", width: 80 },
    { title: "手机号", dataIndex: "phone", key: "phone", width: 150 },
    {
      title: "群内状态",
      dataIndex: "status",
      key: "status",
      render: s => {
        const map = {
          'not_in_group': <Tag color="default">未加群</Tag>,
          'member': <Tag color="blue">普通成员</Tag>,
          'admin': <Tag color="purple">管理员</Tag>,
          'creator': <Tag color="gold">创建者</Tag>,
          'error': <Tag color="red">状态异常</Tag>,
          'invalid_session': <Tag color="red">协议失效</Tag>
        };
        return map[s] || <Tag>{s}</Tag>;
      }
    },
    {
      title: "拉人权限",
      key: "can_invite",
      render: (_, r) => {
        if (r.status === 'not_in_group' || r.status === 'invalid_session') return '-';
        return r.can_invite ? <Tag color="green">有</Tag> : <Tag color="red">无</Tag>;
      }
    },
    { title: "异常信息", dataIndex: "error", key: "error", ellipsis: true, render: t => <Tooltip title={t}>{t || '-'}</Tooltip> }
  ];

  return (
    <Row gutter={24}>
      <TaskDetailDrawer taskId={detailTaskId} onClose={() => setDetailTaskId(null)} />
      <Col span={10}>
        <Card title="账号群内状态鉴别">
          <div style={{ marginBottom: 10 }}>
            <div style={{ marginBottom: 4 }}>目标群链接:</div>
            <Input
              placeholder="https://t.me/xxxx 或 https://t.me/+xxxx"
              value={groupLink}
              onChange={e => setGroupLink(e.target.value)}
            />
            <Button type="primary" onClick={handleCheckAccounts} style={{ marginTop: 8 }} block loading={checkingAccounts}>
              刷新/鉴别账号群内状态
            </Button>
          </div>
          <Table
            dataSource={accounts}
            columns={accountColumns}
            rowKey="session_id"
            size="small"
            pagination={{ pageSize: 10 }}
            style={{ marginTop: 16 }}
          />
        </Card>
        
        <Card title="创建邀请入群任务" style={{ marginTop: 24 }} extra={
          <Upload beforeUpload={handleFileUpload} showUploadList={false}>
            <Tooltip title="导入 TXT/CSV">
              <Button icon={<FileTextOutlined />} size="small" />
            </Tooltip>
          </Upload>
        }>
          <div style={{ marginBottom: 10, padding: 10, borderRadius: 6, background: '#fafafa', border: '1px solid #f0f0f0', color: '#555', fontSize: 12, lineHeight: 1.7 }}>
            注意：脚本会自动从账号列表中筛选出状态为“管理员/创建者”且拥有“拉人权限”的账号去执行邀请任务。请确保你已在客户端手动为这些账号设置了管理员及邀请权限。
          </div>
          <div style={{ marginBottom: 10 }}>
            <div style={{ marginBottom: 4 }}>邀请目标 (每行一个 @username):</div>
            <TextArea
              rows={8}
              placeholder="@user1\n@user2"
              value={targets}
              onChange={e => setTargets(e.target.value)}
            />
            <div style={{ textAlign: 'right', fontSize: 12, color: '#888', marginTop: 4 }}>
              共 {targets ? targets.split('\\n').filter(t => t.trim()).length : 0} 个目标
            </div>
          </div>

          <Form form={form} layout="vertical" onFinish={onFinish} initialValues={{ delay_seconds: 30, max_per_account: 20, random_delay: true }}>
            <Row gutter={16}>
              <Col span={12}>
                <Form.Item label="间隔 (秒)" name="delay_seconds">
                  <InputNumber min={1} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={12}>
                <Form.Item label="单号上限" name="max_per_account">
                  <InputNumber min={1} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
            </Row>
            <Form.Item name="random_delay" valuePropName="checked">
              <Switch checkedChildren="随机延迟" unCheckedChildren="固定延迟" />
            </Form.Item>
            <Button type="primary" htmlType="submit" icon={<UsergroupAddOutlined />} block>
              创建邀请任务
            </Button>
          </Form>
        </Card>
      </Col>
      <Col span={14}>
        <Card title="邀请任务列表" style={{ marginBottom: 24 }}>
          <Table
            dataSource={tasks}
            columns={taskColumns}
            rowKey="id"
            pagination={{ pageSize: 5 }}
            size="small"
          />
        </Card>

        {selectedTaskId && (
          <div style={{ marginBottom: 24 }}>
            <h4>任务 #{selectedTaskId} 日志</h4>
            <LogViewer embedded taskId={selectedTaskId} />
          </div>
        )}
        {!selectedTaskId && <LogViewer embedded />}
      </Col>
    </Row>
  );
};
"""

pattern = re.compile(r"const InviteManager = \(\) => \{.*?(?=const LogStats = \(\) => \{)", re.DOTALL)
new_content = pattern.sub(new_invite_manager + "\n\n", content)

with open("/Users/pclucky/私聊/frontend/src/App.jsx", "w") as f:
    f.write(new_content)
