package com.za.apps.ljemail;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将每个文件夹的所有文件合成一个文件，每一个文件是一行，并且开头是对应的邮件类别
 */
public class EmailDataEtl {
    //文件名-类别(正常邮件0,垃圾邮件1)
    public static Map<String,String>  index_types = new HashMap<String, String>();

    public static void main(String[] args) throws Exception {
        getIndex_data("D:\\trec06c\\email_data\\full\\index");
        transform_data("D:\\trec06c\\email_data\\data","D:\\workspace\\SparkProject\\data\\email_data");
    }

    public static void transform_data(String filepath,String writePath) throws Exception {
        File file = new File(filepath);
        File[] childrens = file.listFiles();
        if(null != childrens && childrens.length>0){
            for(File child:childrens){
                String childName =  child.getName();
                File[] childrenss = child.listFiles();
                StringBuilder result = new StringBuilder();
                if(null != childrenss && childrenss.length>0){
                    for(File cc:childrenss){
                        String name = cc.getName();
                        String index= childName+"_"+name;
                        BufferedReader br =new BufferedReader(new InputStreamReader(new FileInputStream(cc.getAbsoluteFile()),"GBK"));  ;//构造一个BufferedReader类来读取文件
                        String s = null;
                        String type=null;
                        StringBuilder result1 = new StringBuilder();
                        while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                            result1.append(s);
                        }
                        result.append(index_types.get(index)).append(":").append(result1).append("\n");
                        br.close();
                    }
                }
                FileWriter fw = new FileWriter(writePath+File.separator+childName);
                fw.write(result.toString());
                fw.close();
            }
        }
    }

    public static void  getIndex_data(String filepath){
        try{
            BufferedReader br = new BufferedReader(new FileReader(filepath));//构造一个BufferedReader类来读取文件
            String s = null;
            String type=null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                type="0";
                String[] datas = s.split(" ");
                if("spam".equals(datas[0].toLowerCase())){
                    type="1";
                }
                String[] ds = datas[1].split("/");
                String index = ds[2]+"_"+ds[3];
                index_types.put(index,type);
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}
