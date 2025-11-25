import { useEffect, useState } from 'react';
import { 
  Table, Button, Modal, Form, Input, Select, message, 
  Popconfirm, InputNumber, Space, Card, Row, Col 
} from 'antd';
import { 
  DeleteOutlined, EditOutlined, PlusOutlined, 
  SearchOutlined, ReloadOutlined 
} from '@ant-design/icons';

// 引入生成的 Service 和类型
import { TeacherService } from '../../client';
import type { 
  TeacherResp, 
  TeacherCreateParams, 
  TeacherUpdateParams 
} from '../../client';

const { Option } = Select;

export const TeacherManager = () => {
  // === 1. 状态定义 ===
  const [teachers, setTeachers] = useState<TeacherResp[]>([]);
  const [loading, setLoading] = useState(false);
  
  // 搜索状态
  const [searchId, setSearchId] = useState<number | undefined>();
  const [searchName, setSearchName] = useState<string>('');

  // 弹窗相关状态
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isEditMode, setIsEditMode] = useState(false);
  const [currentId, setCurrentId] = useState<number | null>(null);
  
  const [form] = Form.useForm();

  // === 2. 获取数据函数 (支持搜索) ===
  const fetchTeachers = async () => {
    setLoading(true);
    try {
      const res = await TeacherService.searchTeacherApiV1TeachersGet(
        searchId,   // id 参数
        searchName || undefined // name 参数
      );
      setTeachers(res.result);
      
      // 友好提示
      if (res.result.length === 0 && (searchId || searchName)) {
        message.info('未找到匹配的教师');
      }
    } catch (error) {
      console.error(error);
      message.error('获取数据失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchTeachers(); }, []);

  const openAddModal = () => {
    setIsEditMode(false);
    setCurrentId(null);
    form.resetFields();
    setIsModalOpen(true);
  };

  const openEditModal = (record: TeacherResp) => {
    setIsEditMode(true);
    setCurrentId(record.teacher_id); // 注意字段名是 teacher_id
    form.setFieldsValue({
      year: undefined, // 编辑模式不需要
      name: record.name,
      sex: record.sex,
      age: record.age,
      // 教师没有 current_campus
    });
    setIsModalOpen(true);
  };

  // === 3. 核心功能：添加 ===
  const handleAdd = async () => {
    try {
      const values = await form.validateFields();
      // 构造创建参数
      const createData: TeacherCreateParams = {
        year: values.year,
        name: values.name,
        sex: values.sex,
        age: values.age,
      };
      
      await TeacherService.addTeacherApiV1TeachersPost(createData);
      message.success('添加成功');
      setIsModalOpen(false);
      fetchTeachers();
    } catch (error) {
      message.error('添加失败');
    }
  };

  // === 4. 核心功能：更新 ===
  const handleUpdate = async () => {
    try {
      const values = await form.validateFields();
      if (currentId === null) return;

      const updateData: TeacherUpdateParams = {
        name: values.name,
        sex: values.sex,
        age: values.age
      };

      await TeacherService.updateTeacherApiV1TeachersTeacherIdPut(currentId, updateData);
      message.success('修改成功');
      setIsModalOpen(false);
      fetchTeachers();
    } catch (error) {
      message.error('修改失败');
    }
  };

  const handleOk = () => {
    if (isEditMode) {
      handleUpdate();
    } else {
      handleAdd();
    }
  };

  // === 5. 核心功能：删除 ===
  const handleDelete = async (id: number) => {
    try {
      await TeacherService.deleteTeacherApiV1TeachersTeacherIdDelete(id);
      message.success('删除成功');
      fetchTeachers();
    } catch (error) {
      message.error('删除失败');
    }
  };

  // === 6. 表格列定义 ===
  const columns: any = [
    { title: '工号', dataIndex: 'teacher_id', width: 150 }, // 对应 TeacherResp.teacher_id
    { title: '姓名', dataIndex: 'name', key: 'name' },
    { title: '性别', dataIndex: 'sex', key: 'sex', width: 80 },
    { title: '年龄', dataIndex: 'age', width: 80 },
    {
      title: '',
      align: 'right',
      render: (_: any, record: TeacherResp) => (
        <Space>
          <Button icon={<EditOutlined />} onClick={() => openEditModal(record)}>
            编辑
          </Button>
          <Popconfirm title="确认删除?" onConfirm={() => handleDelete(record.teacher_id)}>
            <Button danger type="link" icon={<DeleteOutlined />}>删除</Button>
          </Popconfirm>
        </Space>
      )
    }
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16 }} bodyStyle={{ padding: '16px 24px' }}>
        <Row gutter={16} align="middle">
          <Col>
            <InputNumber 
              placeholder="搜索工号 (精确)" 
              style={{ width: 180 }} 
              value={searchId}
              onChange={(val) => setSearchId(val ?? undefined)}
              onPressEnter={fetchTeachers}
            />
          </Col>
          <Col>
            <Input 
              placeholder="搜索姓名 (模糊)" 
              style={{ width: 180 }} 
              value={searchName}
              onChange={e => setSearchName(e.target.value)}
              onPressEnter={fetchTeachers}
            />
          </Col>
          <Col>
            <Button type="primary" icon={<SearchOutlined />} onClick={fetchTeachers}>
              查询
            </Button>
          </Col>
          <Col>
            <Button icon={<ReloadOutlined />} onClick={() => {
              setSearchId(undefined);
              setSearchName('');
              window.location.reload(); // 简单重置
            }}>
              重置
            </Button>
          </Col>
          <Col style={{ marginLeft: 'auto' }}>
            <Button type="primary" icon={<PlusOutlined />} onClick={openAddModal}>
              添加教师
            </Button>
          </Col>
        </Row>
      </Card>

      <Table 
        dataSource={teachers} 
        columns={columns} 
        rowKey="teacher_id" 
        loading={loading} 
        pagination={{ pageSize: 10 }}
      />
      
      <Modal 
        title={isEditMode ? "编辑教师信息" : "添加教师"} 
        open={isModalOpen} 
        onOk={handleOk} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical">
          {!isEditMode && (
            <Form.Item name="year" label="入职年份" rules={[{ required: true }]}>
              <InputNumber style={{ width: '100%' }} min={2000} max={2099} />
            </Form.Item>
          )}
          <Form.Item name="name" label="姓名" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="sex" label="性别" rules={[{ required: true }]}>
            <Select>
              <Select.Option value="M">男</Select.Option>
              <Select.Option value="F">女</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item name="age" label="年龄" rules={[{ required: true }]}>
            <InputNumber min={20} style={{ width: '100%' }} />
          </Form.Item>
          {/* 教师没有校区字段 */}
        </Form>
      </Modal>
    </div>
  );
};