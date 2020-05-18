package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/grpc-demo/blog/blogpb"
	"google.golang.org/grpc"
)

func main() {
	fmt.Println("Creation of client has started")
	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalln("Couldn not connect with server", err)
	}
	defer cc.Close()

	c := blogpb.NewBlogServiceClient(cc)

	//create Blog
	fmt.Println("Creating a blog")
	blog := &blogpb.Blog{
		AuthorId: "Hemant",
		Content:  "My First blog on the server",
		Title:    "First Blog",
	}
	res, err := c.CreateBlog(context.Background(), &blogpb.CreateBlogRequest{Blog: blog})

	if err != nil {
		log.Fatalln("Unexpected error:", err)
	}

	fmt.Println("Blog has been created:", res)

	//read Blog
	fmt.Println("Reading a blog")
	_, err2 := c.ReadBlog(context.Background(), &blogpb.ReadBlogRequest{BlogId: "1234"})

	if err2 != nil {
		fmt.Println("Error reading the blog:", err2)
	}

	readRes, err3 := c.ReadBlog(context.Background(), &blogpb.ReadBlogRequest{BlogId: res.GetBlog().GetId()})

	if err3 != nil {
		fmt.Println("Error reading the blog:", err3)
	}

	fmt.Println("Blog has been read:", readRes)

	//Update Blog
	newBlog := &blogpb.Blog{
		Id:       res.GetBlog().GetId(),
		AuthorId: "Bharat",
		Content:  "The content has been changed",
		Title:    "The changed blog",
	}

	updateRes, err4 := c.UpdateBlog(context.Background(), &blogpb.UpdateBlogRequest{Blog: newBlog})

	if err4 != nil {
		fmt.Println("Error while updating the blog:", err4)
	}

	fmt.Println("The updated blog is:", updateRes.GetBlog())

	//Delete Blog
	deleteRes, err5 := c.DeleteBlog(context.Background(), &blogpb.DeleteBlogRequest{BlogId: res.GetBlog().GetId()})

	if err5 != nil {
		fmt.Println("Error while deleting the blog:", err5)
	}

	fmt.Println("Deletion was successful and deleted blog had id:", deleteRes.GetBlogId())

	//list Blogs
	stream, err6 := c.ListBlog(context.Background(), &blogpb.ListBlogRequest{})

	if err6 != nil {
		fmt.Println("Error while calling ListBlog RPC:", err6)
	}
	for {
		listRes, err7 := stream.Recv()
		if err7 == io.EOF {
			break
		}
		if err7 != nil {
			log.Fatalln("Error while retreiving items:", err7)
		}
		fmt.Println(listRes.GetBlog())
	}
}
