import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Popconfirm, Table, Tag, Upload, message } from 'antd';
import { DeleteOutlined, SyncOutlined, UploadOutlined } from '@ant-design/icons';
import { batchCheckSessions, batchDeleteSessions, checkSession, getSessionOtp, getSessions, login, sendCode, updateProfile, uploadSession } from './api';

const { TextArea } = Input;

const SessionManager = () => {
  const [sessions, setSessions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);
  const [isProfileModalOpen, setIsProfileModalOpen] = useState(false);
  const [profileForm] = Form.useForm();
  const [phone, setPhone] = useState("");
  const [code, setCode] = useState("");
  const [phoneHash, setPhoneHash] = useState("");
  const [tempSession, setTempSession] = useState("");
  const [loginApiId, setLoginApiId] = useState(null);
  const [loginApiHash, setLoginApiHash] = useState(null);
  const [manualApiId, setManualApiId] = useState("34995631");
  const [manualApiHash, setManualApiHash] = useState("49bff8c0eea73a487798b23d089c1b71");
  const [loginPassword, setLoginPassword] = useState("");
  const [step, setStep] = useState(1);
  const [managerModalOpen, setManagerModalOpen] = useState(false);
  const [managerPhone, setManagerPhone] = useState("");
  const [managerCode, setManagerCode] = useState("");
  const [managerPhoneHash, setManagerPhoneHash] = useState("");
  const [managerTempSession, setManagerTempSession] = useState("");
  const [managerLoginApiId, setManagerLoginApiId] = useState(null);
  const [managerLoginApiHash, setManagerLoginApiHash] = useState(null);
  const [managerManualApiId, setManagerManualApiId] = useState("34995631");
  const [managerManualApiHash, setManagerManualApiHash] = useState("49bff8c0eea73a487798b23d089c1b71");
  const [managerLoginPassword, setManagerLoginPassword] = useState("");
  const [managerStep, setManagerStep] = useState(1);
  const [isUploadModalOpen, setIsUploadModalOpen] = useState(false);
  const [uploadApiId, setUploadApiId] = useState("35019294");
  const [uploadApiHash, setUploadApiHash] = useState("9e2d91fe6876d834bae4707b0875e2d7");
  const [fileList, setFileList] = useState([]);

  const loadSessions = async () => {
    setLoading(true);
    try {
      const res = await getSessions();
      setSessions(res.items);
    } catch (e) {
      message.error("Failed to load sessions");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadSessions(); }, []);

  const handleUploadSubmit = async () => {
    if (!uploadApiId || !uploadApiHash) {
      message.error("请输入 API ID 和 API Hash");
      return;
    }
    if (fileList.length === 0) {
      message.error("请选择文件");
      return;
    }

    const formData = new FormData();
    fileList.forEach(file => {
      formData.append("files", file);
    });
    formData.append("api_id", uploadApiId);
    formData.append("api_hash", uploadApiHash);

    try {
      setLoading(true);
      await uploadSession(formData);
      message.success(`成功上传 ${fileList.length} 个会话`);
      setIsUploadModalOpen(false);
      setFileList([]);
      loadSessions();
    } catch (e) {
      message.error("上传失败: " + e.message);
    } finally {
      setLoading(false);
    }
  };

  const uploadProps = {
    multiple: true,
    onRemove: (file) => {
      setFileList((prev) => {
        const index = prev.indexOf(file);
        const newFileList = prev.slice();
        newFileList.splice(index, 1);
        return newFileList;
      });
    },
    beforeUpload: (_, nextFileList) => {
      setFileList(nextFileList);
      return false;
    },
    fileList,
  };

  const handleSendCode = async () => {
    try {
      if (!phone.trim()) {
        message.error("请输入手机号");
        return;
      }
      if ((manualApiId && !manualApiHash) || (!manualApiId && manualApiHash)) {
        message.error("API ID 和 API Hash 需要同时填写");
        return;
      }
      const res = await sendCode(phone.trim(), manualApiId, manualApiHash);
      setPhoneHash(res.phone_code_hash);
      setTempSession(res.temp_session || "");
      if (res.api_id && res.api_hash) {
        setLoginApiId(res.api_id);
        setLoginApiHash(res.api_hash);
      }
      setStep(2);
      message.success("验证码已发送");
    } catch (e) {
      message.error("发送验证码失败: " + e.message);
    }
  };

  const handleLogin = async () => {
    try {
      await login(phone, code, phoneHash, loginApiId, loginApiHash, loginPassword, tempSession, false);
      message.success("登录成功");
      setIsModalOpen(false);
      loadSessions();
      setStep(1);
      setPhone("");
      setCode("");
      setPhoneHash("");
      setTempSession("");
      setLoginApiId(null);
      setLoginApiHash(null);
      setLoginPassword("");
    } catch (e) {
      message.error("登录失败: " + e.message);
    }
  };

  const handleSendManagerCode = async () => {
    try {
      if (!managerPhone.trim()) {
        message.error("请输入管理号手机号");
        return;
      }
      if ((managerManualApiId && !managerManualApiHash) || (!managerManualApiId && managerManualApiHash)) {
        message.error("API ID 和 API Hash 需要同时填写");
        return;
      }
      const res = await sendCode(managerPhone.trim(), managerManualApiId, managerManualApiHash);
      setManagerPhoneHash(res.phone_code_hash);
      setManagerTempSession(res.temp_session || "");
      if (res.api_id && res.api_hash) {
        setManagerLoginApiId(res.api_id);
        setManagerLoginApiHash(res.api_hash);
      }
      setManagerStep(2);
      message.success("管理号验证码已发送");
    } catch (e) {
      message.error("发送验证码失败: " + e.message);
    }
  };

  const handleManagerLogin = async () => {
    try {
      await login(
        managerPhone,
        managerCode,
        managerPhoneHash,
        managerLoginApiId,
        managerLoginApiHash,
        managerLoginPassword,
        managerTempSession,
        true
      );
      message.success("管理号登录成功");
      setManagerModalOpen(false);
      loadSessions();
      setManagerStep(1);
      setManagerPhone("");
      setManagerCode("");
      setManagerPhoneHash("");
      setManagerTempSession("");
      setManagerLoginApiId(null);
      setManagerLoginApiHash(null);
      setManagerLoginPassword("");
    } catch (e) {
      message.error("管理号登录失败: " + e.message);
    }
  };

  const handleCheck = async (id) => {
    try {
      message.loading({ content: "检查中...", key: "check" });
      const res = await checkSession(id);
      message.success({ content: `状态: ${res.status}`, key: "check" });
      loadSessions();
    } catch (e) {
      message.error({ content: "检查失败", key: "check" });
    }
  };

  const handleGetOtp = async (id) => {
    try {
      message.loading({ content: "正在获取验证码...", key: "otp", duration: 0 });
      const res = await getSessionOtp(id);
      if (res.status === 'success') {
        message.success({ content: "获取成功", key: "otp" });
        Modal.success({
          title: "获取验证码成功",
          content: (
            <div>
              <p>验证码: <b style={{ fontSize: 18, color: 'red' }}>{res.code || "未找到"}</b></p>
              <div style={{ maxHeight: 200, overflow: 'auto', background: '#f5f5f5', padding: 8 }}>
                {res.full_message}
              </div>
              <p style={{ marginTop: 8, fontSize: 12, color: '#999' }}>时间: {res.date}</p>
            </div>
          )
        });
      } else {
        message.error({ content: "获取失败: " + res.message, key: "otp" });
      }
    } catch (e) {
      message.error({ content: "请求失败: " + e.message, key: "otp" });
    }
  };

  const handleBatchCheck = async () => {
    if (selectedRowKeys.length === 0) return message.warning("请选择账号");
    try {
      const res = await batchCheckSessions(selectedRowKeys);
      message.success(res.message || "已在后台开始批量检测");
      setSelectedRowKeys([]);
    } catch (e) {
      message.error("检查失败: " + e.message);
    }
  };

  const handleBatchDelete = async () => {
    if (selectedRowKeys.length === 0) return message.warning("请选择账号");
    try {
      await batchDeleteSessions(selectedRowKeys);
      message.success("删除成功");
      setSelectedRowKeys([]);
      loadSessions();
    } catch (e) {
      message.error("删除失败");
    }
  };

  const handleDeleteBanned = async () => {
    const bannedIds = sessions.filter(s => s.status === 'banned' || s.status === 'restricted' || s.status === 'invalid').map(s => s.id);
    if (bannedIds.length === 0) return message.info("没有发现风控/封禁/无效账号");

    try {
      await batchDeleteSessions(bannedIds);
      message.success(`已删除 ${bannedIds.length} 个风控/无效账号`);
      loadSessions();
    } catch (e) {
      message.error("删除失败");
    }
  };

  const handleUpdateProfileSubmit = async () => {
    try {
      const values = await profileForm.validateFields();
      const formData = new FormData();
      formData.append("ids", selectedRowKeys.join(","));
      if (values.first_name) formData.append("first_name", values.first_name);
      if (values.about) formData.append("about", values.about);
      if (values.avatar && values.avatar.length > 0) {
        formData.append("avatar", values.avatar[0].originFileObj);
      }

      setLoading(true);
      const res = await updateProfile(formData);
      message.success(res.message || "更新完成");
      setIsProfileModalOpen(false);
      profileForm.resetFields();
      loadSessions();
      setSelectedRowKeys([]);
    } catch (e) {
      message.error("更新失败: " + e.message);
    } finally {
      setLoading(false);
    }
  };

  const columns = [
    {
      title: "序号",
      key: "index",
      width: 60,
      render: (text, record, index) => index + 1
    },
    { title: "手机号", dataIndex: "phone", key: "phone" },
    { title: "昵称", dataIndex: "nickname", key: "nickname", render: t => t || '-' },
    {
      title: "角色",
      dataIndex: "is_manager",
      key: "is_manager",
      render: v => v ? <Tag color="purple">管理号</Tag> : <Tag>协议号</Tag>
    },
    { title: "健康分", dataIndex: "health_score", key: "health_score", render: score => <Tag color={score > 80 ? 'green' : score > 50 ? 'orange' : 'red'}>{score}</Tag> },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      render: (status) => {
        let color = status === 'active' ? 'green' : status === 'banned' ? 'red' : 'orange';
        let text = status === 'active' ? '正常' : status === 'banned' ? '封禁' : status;
        return <Tag color={color}>{text.toUpperCase()}</Tag>;
      }
    },
    {
      title: "上次使用",
      dataIndex: "last_used",
      key: "last_used",
      render: (text) => text || "-"
    },
    {
      title: "操作",
      key: "action",
      render: (_, record) => (
        <div style={{ display: 'flex', gap: 8 }}>
          <Button size="small" icon={<SyncOutlined />} onClick={() => handleCheck(record.id)}>检查</Button>
          <Button size="small" onClick={() => handleGetOtp(record.id)}>取码</Button>
        </div>
      )
    }
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        <Button icon={<UploadOutlined />} onClick={() => setIsUploadModalOpen(true)}>上传</Button>
        <Button type="primary" onClick={() => setIsModalOpen(true)}>登录</Button>
        <Button type="primary" ghost onClick={() => setManagerModalOpen(true)}>管理号登录</Button>
        <Button icon={<SyncOutlined />} onClick={loadSessions}>刷新</Button>
        <Button onClick={() => setIsProfileModalOpen(true)} disabled={selectedRowKeys.length === 0}>批量修改资料</Button>
        <Button onClick={handleBatchCheck} disabled={selectedRowKeys.length === 0}>批量检测</Button>
        <Popconfirm title="确定删除选中账号?" onConfirm={handleBatchDelete}>
          <Button danger disabled={selectedRowKeys.length === 0} icon={<DeleteOutlined />}>批量删除</Button>
        </Popconfirm>
        <Popconfirm title="确定删除所有风控/封禁账号?" onConfirm={handleDeleteBanned}>
          <Button danger>删除风控账号</Button>
        </Popconfirm>
      </div>

      <Table
        rowSelection={{
          selectedRowKeys,
          onChange: setSelectedRowKeys,
        }}
        dataSource={sessions}
        columns={columns}
        rowKey="id"
        loading={loading}
        pagination={false}
      />

      <Modal
        title={`批量修改资料 (已选 ${selectedRowKeys.length} 个)`}
        open={isProfileModalOpen}
        onOk={handleUpdateProfileSubmit}
        onCancel={() => setIsProfileModalOpen(false)}
        okText="开始修改"
        cancelText="取消"
      >
        <Form form={profileForm} layout="vertical">
          <Form.Item label="新昵称 (First Name)" name="first_name">
            <Input placeholder="留空则不修改" />
          </Form.Item>
          <Form.Item label="新简介 (About)" name="about">
            <TextArea placeholder="留空则不修改" rows={2} />
          </Form.Item>
          <Form.Item label="新头像" name="avatar" valuePropName="fileList" getValueFromEvent={e => {
            if (Array.isArray(e)) return e;
            return e && e.fileList;
          }}>
            <Upload beforeUpload={() => false} maxCount={1} listType="picture">
              <Button icon={<UploadOutlined />}>选择图片</Button>
            </Upload>
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="上传会话文件 (支持批量)"
        open={isUploadModalOpen}
        onOk={handleUploadSubmit}
        onCancel={() => {
          setIsUploadModalOpen(false);
          setFileList([]);
        }}
        okText="开始上传"
        cancelText="取消"
      >
        <div style={{ marginBottom: 16 }}>
          <div style={{ marginBottom: 8 }}>API ID:</div>
          <Input
            value={uploadApiId}
            onChange={e => setUploadApiId(e.target.value)}
            placeholder="例如: 123456"
          />
        </div>
        <div style={{ marginBottom: 16 }}>
          <div style={{ marginBottom: 8 }}>API Hash:</div>
          <Input
            value={uploadApiHash}
            onChange={e => setUploadApiHash(e.target.value)}
            placeholder="例如: abcdef123456..."
          />
        </div>
        <Upload {...uploadProps}>
          <Button icon={<UploadOutlined />}>选择文件 (.session 或 .zip)</Button>
        </Upload>
      </Modal>

      <Modal title="手机号登录" open={isModalOpen} onCancel={() => setIsModalOpen(false)} footer={null}>
        {step === 1 ? (
          <div>
            <Input placeholder="手机号 (例如 +8613800000000)" value={phone} onChange={e => setPhone(e.target.value)} />
            <Input style={{ marginTop: 10 }} placeholder="API ID（手动登录群主号建议填写）" value={manualApiId} onChange={e => setManualApiId(e.target.value)} />
            <Input style={{ marginTop: 10 }} placeholder="API Hash（手动登录群主号建议填写）" value={manualApiHash} onChange={e => setManualApiHash(e.target.value)} />
            <Button type="primary" onClick={handleSendCode} style={{ marginTop: 10 }} block>发送验证码</Button>
          </div>
        ) : (
          <div>
            <Input placeholder="验证码" value={code} onChange={e => setCode(e.target.value)} />
            <Input.Password placeholder="二级密码（若已开启）" value={loginPassword} onChange={e => setLoginPassword(e.target.value)} style={{ marginTop: 10 }} />
            <Button type="primary" onClick={handleLogin} style={{ marginTop: 10 }} block>登录</Button>
          </div>
        )}
      </Modal>

      <Modal title="管理号登录" open={managerModalOpen} onCancel={() => setManagerModalOpen(false)} footer={null}>
        {managerStep === 1 ? (
          <div>
            <Input placeholder="管理号手机号 (例如 +8613800000000)" value={managerPhone} onChange={e => setManagerPhone(e.target.value)} />
            <Input style={{ marginTop: 10 }} placeholder="API ID（建议管理号专用）" value={managerManualApiId} onChange={e => setManagerManualApiId(e.target.value)} />
            <Input style={{ marginTop: 10 }} placeholder="API Hash（建议管理号专用）" value={managerManualApiHash} onChange={e => setManagerManualApiHash(e.target.value)} />
            <Button type="primary" onClick={handleSendManagerCode} style={{ marginTop: 10 }} block>发送验证码</Button>
          </div>
        ) : (
          <div>
            <Input placeholder="验证码" value={managerCode} onChange={e => setManagerCode(e.target.value)} />
            <Input.Password placeholder="二级密码（若已开启）" value={managerLoginPassword} onChange={e => setManagerLoginPassword(e.target.value)} style={{ marginTop: 10 }} />
            <Button type="primary" onClick={handleManagerLogin} style={{ marginTop: 10 }} block>登录管理号</Button>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default SessionManager;
