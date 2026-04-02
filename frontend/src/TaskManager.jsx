import React, { useEffect, useState } from 'react';
import { Button, Card, Col, Form, Input, InputNumber, Popconfirm, Progress, Row, Switch, Table, Tag, Tooltip, Upload, message } from 'antd';
import { DeleteOutlined, FileTextOutlined, PauseCircleOutlined, SendOutlined } from '@ant-design/icons';
import { createTask, deleteTask, getTasks, restartTask, stopTask } from './api';
import LogViewer from './LogViewer';
import TaskDetailDrawer from './TaskDetailDrawer';

const { TextArea } = Input;

const TaskManager = () => {
  const [form] = Form.useForm();
  const [targets, setTargets] = useState("");
  const [messageTemplate, setMessageTemplate] = useState("");
  const [tasks, setTasks] = useState([]);
  const [selectedTaskId, setSelectedTaskId] = useState(null);
  const [detailTaskId, setDetailTaskId] = useState(null);

  const loadTasks = async () => {
    try {
      const res = await getTasks('dm');
      setTasks(res.items);
    } catch (e) {
      return;
    }
  };

  useEffect(() => {
    loadTasks();
    const interval = setInterval(loadTasks, 5000);
    return () => clearInterval(interval);
  }, []);

  const onFinish = async (values) => {
    const targetList = targets.split('\n').map(t => t.trim()).filter(t => t);
    if (targetList.length === 0) {
      message.error("请至少输入一个目标用户");
      return;
    }

    const uniqueTargets = [...new Set(targetList)];
    if (uniqueTargets.length !== targetList.length) {
      message.warning(`检测到重复目标，已自动去重。原: ${targetList.length}, 去重后: ${uniqueTargets.length}`);
    }

    const payload = {
      message: messageTemplate,
      targets: uniqueTargets,
      delay_seconds: values.delay_seconds,
      random_delay: values.random_delay,
      max_per_account: values.max_per_account
    };

    try {
      await createTask(payload);
      message.success(`任务已创建，共 ${uniqueTargets.length} 个目标`);
      form.resetFields();
      setTargets("");
      setMessageTemplate("");
      loadTasks();
    } catch (e) {
      message.error("任务创建失败");
    }
  };

  const handleFileUpload = (file) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const text = e.target.result;
      setTargets(text);
      message.success(`已加载 ${text.split('\n').length} 行数据`);
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
    {
      title: "消息预览",
      dataIndex: "message",
      key: "message",
      ellipsis: true,
      render: (text) => <Tooltip title={text}>{text.slice(0, 20)}...</Tooltip>
    },
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
          <Button size="small" type="primary" onClick={() => setDetailTaskId(record.id)}>待发送</Button>
          <Button size="small" onClick={() => setSelectedTaskId(record.id)}>日志</Button>
          {record.status === 'running' && (
            <Popconfirm title="确定停止任务?" onConfirm={() => handleStop(record.id)}>
              <Button size="small" icon={<PauseCircleOutlined />} danger>停止</Button>
            </Popconfirm>
          )}
          {(record.status === 'stopped' || record.status === 'failed' || record.status === 'completed') && (
            <Popconfirm title="确定重启任务? 已发送目标将跳过。" onConfirm={() => handleRestart(record.id)}>
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

  return (
    <Row gutter={24}>
      <TaskDetailDrawer taskId={detailTaskId} onClose={() => setDetailTaskId(null)} />
      <Col span={8}>
        <Card title="创建新任务" extra={
          <Upload beforeUpload={handleFileUpload} showUploadList={false}>
            <Tooltip title="导入 TXT/CSV">
              <Button icon={<FileTextOutlined />} size="small" />
            </Tooltip>
          </Upload>
        }>
          <div style={{ marginBottom: 10 }}>
            <div style={{ marginBottom: 4 }}>目标列表 (每行一个 @username 或手机号):</div>
            <TextArea
              rows={10}
              placeholder="@user1&#10;@user2&#10;+8613800000000"
              value={targets}
              onChange={e => setTargets(e.target.value)}
            />
            <div style={{ textAlign: 'right', fontSize: 12, color: '#888', marginTop: 4 }}>
              共 {targets ? targets.split('\n').filter(t => t.trim()).length : 0} 个目标
            </div>
          </div>

          <Form form={form} layout="vertical" onFinish={onFinish} initialValues={{ delay_seconds: 30, max_per_account: 20, random_delay: true }}>
            <Form.Item label="消息模板 (支持 Spintax {Hi|Hello})" name="message_template">
              <TextArea
                rows={4}
                value={messageTemplate}
                onChange={e => setMessageTemplate(e.target.value)}
                placeholder="你好 {朋友|兄弟}..."
              />
            </Form.Item>

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

            <Button type="primary" htmlType="submit" icon={<SendOutlined />} block>
              开始任务
            </Button>
          </Form>
        </Card>
      </Col>
      <Col span={16}>
        <Card title="任务列表" style={{ marginBottom: 24 }}>
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
            <h4>任务 #{selectedTaskId} 详情</h4>
            <LogViewer embedded taskId={selectedTaskId} />
          </div>
        )}
        {!selectedTaskId && <LogViewer embedded />}
      </Col>
    </Row>
  );
};

export default TaskManager;
