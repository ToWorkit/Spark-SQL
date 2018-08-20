package com.imooc.dao;

import com.imooc.domain.VideoAccessTopN;
import com.imooc.utils.MySQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 面向接口编程
 */
public class VideoAccessTopNDAO {

    /**
     * 静态初始化课程名称
     */
    static Map<String, String> couress = new HashMap<String, String>();
    static {
        couress.put("4000", "MySQL");
        couress.put("4500", "Linux");
        couress.put("4600", "JavaScript");
        couress.put("14540", "Java");
        couress.put("14390", "Scala");
        couress.put("14322", "Python");
        couress.put("14704", "Solidity");
        couress.put("14623", "人工智能");
    }

    /**
     * 根据课程编号查询课程名称
     * @param id
     * @return
     */
    public String getCourseName(String id) {
        return couress.get(id);
    }


    /**
     * 根据day查询当天的受欢迎的Top5的课程
     * @param day
     * @return
     */
    public List<VideoAccessTopN> query(String day) {
        List<VideoAccessTopN> list = new ArrayList<VideoAccessTopN>();

        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            connection = MySQLUtils.getConnection();
			// 格式化
            String sql = "select cms_id, times from day_video_access_topn_stat where day = ? order by times desc limit 5";
            pstmt = connection.prepareStatement(sql);
            // 设置 ? 的值
            pstmt.setString(1, day);
            // 执行查询
            rs = pstmt.executeQuery();

            VideoAccessTopN domain = null;
            while (rs.next()) {
                domain = new VideoAccessTopN();


                // 需要的name和value的值
                /**
                 * 需要通过cms_id获取课程的名称
                 * 编号和名称是有一个对应的关系，一般是存放在关系型数据库中
                 */
                domain.setName(getCourseName(rs.getLong("cms_id") + ""));
                domain.setValue(rs.getLong("times"));


                list.add(domain);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            MySQLUtils.release(connection, pstmt, rs);
        }
        return list;
    }

    public static void main(String[] args) {
        VideoAccessTopNDAO dao = new VideoAccessTopNDAO();
        List<VideoAccessTopN> list = dao.query("20170511");
        for (VideoAccessTopN result: list) {
            System.out.println(result.getName() + "," + result.getValue());
        }
    }
}
