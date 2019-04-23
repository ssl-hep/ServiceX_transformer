int printXaodBranches(TString inFile, TString inBranch) {
    TStopwatch sw = TStopwatch();
    sw.Start();

    xAOD::Init();
    
    TFile* f = TFile::Open(inFile);
    TTree* t = xAOD::MakeTransientTree(f);
    gSystem->RedirectOutput("temp.txt", "w");
    // t->Show();
    t->Print();
    gSystem->RedirectOutput(0);

    t->SetBranchStatus("*", 0);
    t->SetBranchStatus(inBranch, 1);
    std::cout << "Number of input events: " << t->GetEntries() << std::endl;
    for(int jEntry = 0; jEntry < t->GetEntries(); jEntry++) {
        t->GetEntry(jEntry);

        if(jEntry % 1000 == 0) {
            std::cout << "Processing entry " << jEntry << std::endl;
        }
    }
    
    xAOD::ClearTransientTrees();

    sw.Stop();
    std::cout << "Real time: " << sw.RealTime() / 60.0 << " minutes" << std::endl;
    std::cout << "CPU time:  " << sw.CpuTime() / 60.0 << " minutes" << std::endl;
    return 0;
}
