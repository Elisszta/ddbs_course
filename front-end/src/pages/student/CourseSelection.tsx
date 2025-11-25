import { useEffect, useState } from 'react';
import { Table, Button, message, Tag, Radio } from 'antd';
import { CourseApiService } from '../../client';
import type { CourseResp } from '../../client'; // Fix: 使用 type 导入类型

export const CourseSelection = () => {
  const [courses, setCourses] = useState<CourseResp[]>([]);
  const [campus, setCampus] = useState<'A' | 'B' | 'C'>('A');
  const [loading, setLoading] = useState(false);

  const fetchCourses = async () => {
    setLoading(true);
    try {
      // Query Courses 接口
      // 后端会自动从 Token 中识别当前学生 ID，因此不需要手动传 userId
      const res = await CourseApiService.queryCoursesApiV1CoursesGet(
        [campus]
      );
      setCourses(res.result);
    } catch (error) {
      message.error('获取课程失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchCourses();
  }, [campus]);

  const handleSelect = async (courseId: number, isSelected: boolean | null | undefined) => {
    try {
      const userId = JSON.parse(localStorage.getItem('user') || '{}').user_id;
      if (isSelected) {
        // 退课
        await CourseApiService.deselectCourseApiV1CoursesCourseIdDeselectPost(courseId, userId);
        message.success('退课成功');
      } else {
        // 选课
        await CourseApiService.selectCourseApiV1CoursesCourseIdSelectPost(courseId, userId);
        message.success('抢课成功');
      }
      fetchCourses();
    } catch (error: any) {
      // 你的后端 409 会返回 GenericError
      message.error(error.body?.detail?.msg || '操作失败');
    }
  };

  const columns = [
    { title: 'ID', dataIndex: 'course_id' },
    { title: '名称', dataIndex: 'name' },
    { title: '容量', render: (r: CourseResp) => `${r.num_selected}/${r.capacity}` },
    { title: '教师', dataIndex: 'teachers' },
    { 
      title: '操作', 
      render: (r: CourseResp) => (
        <Button 
          type={r.is_selected ? 'default' : 'primary'}
          danger={!!r.is_selected}
          disabled={!r.is_selected && r.num_selected >= r.capacity}
          onClick={() => handleSelect(r.course_id, r.is_selected)}
        >
          {r.is_selected ? '退课' : r.num_selected >= r.capacity ? '已满' : '抢课'}
        </Button>
      ) 
    }
  ];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <span style={{ marginRight: 8 }}>切换校区:</span>
        <Radio.Group value={campus} onChange={e => setCampus(e.target.value)}>
          <Radio.Button value="A">A 校区</Radio.Button>
          <Radio.Button value="B">B 校区</Radio.Button>
          <Radio.Button value="C">C 校区</Radio.Button>
        </Radio.Group>
      </div>
      <Table dataSource={courses} columns={columns} rowKey="course_id" loading={loading} />
    </div>
  );
};