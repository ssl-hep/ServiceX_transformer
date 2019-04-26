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

    TFile* f_out = new TFile("test.root", "recreate");
    TTree* t_out = new TTree("testTree", "Tree for testing");

    // Hard code some example branches for now
    // TClass* cl = TClass::GetClass("xAOD::ElectronContainer");
    // xAOD::ElectronContainer Electrons;
    DataVector<xAOD::Electron_v1> Electrons;
    TBranch* b_Electrons;
    t->SetBranchAddress("Electrons", & Electrons, & b_Electrons);

    Int_t n_electrons;
    std::vector<Float_t>* electron_pt;

    t_out->Branch("n_electrons", & n_electrons, "n_electrons/I");
    t_out->Branch("electron_pt", electron_pt);

    for(int jEntry = 0; jEntry < t->GetEntries(); jEntry++) {
        t->GetEntry(jEntry);

        if(jEntry % 1000 == 0) {
            std::cout << "Processing entry " << jEntry << std::endl;
        }

        // n_electrons = t->Electrons.size();
        // for(int i = 0; i < t->Electrons.size(); i++) {
        //     electron_pt->push_back(t->Electrons.at(i));
        // }

        // t_out->Fill();
    }

    f_out->Write();
    f_out->Close();
    
    xAOD::ClearTransientTrees();

    sw.Stop();
    std::cout << "Real time: " << sw.RealTime() / 60.0 << " minutes" << std::endl;
    std::cout << "CPU time:  " << sw.CpuTime() / 60.0 << " minutes" << std::endl;
    return 0;
}
